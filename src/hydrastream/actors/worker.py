# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import os
import random
import traceback
from abc import ABC, abstractmethod
from typing import cast

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream._curl_shim import aiter_bytes, get_error_response
from hydrastream.actors.controller import (
    MaxLimitSignal,
    NetworkCongestionSignal,
    TrafficSignal,
)
from hydrastream.actors.dispatcher import FileCompleted
from hydrastream.actors.stater import ProgressDeltaCmd, RemoveFileCmd, StateKeeperCmd
from hydrastream.actors.throttler import (
    RegisterStreamCmd,
    RemoveStreamCmd,
    ThrottlerMsg,
)
from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    StreamError,
    WorkerScaleDown,
)
from hydrastream.interfaces import StorageBackend
from hydrastream.models import (
    Chunk,
    Envelope,
    File,
    NetworkState,
    StopMsg,
    StreamChunk,
    UIState,
    WriteChunk,
    my_dataclass,
)
from hydrastream.monitor import done, log
from hydrastream.network import stream_chunk, try_scale_up


@my_dataclass
class BaseDownloadWorker(ABC):
    chunks_inbox: asyncio.PriorityQueue[Envelope[Chunk | StopMsg]]
    throttler_outbox: asyncio.Queue[ThrottlerMsg] | None = None
    controller_outbox: asyncio.Queue[TrafficSignal]
    state_outbox: asyncio.Queue[StateKeeperCmd]

    wakeup_event: asyncio.Event
    all_complete: asyncio.Event

    barrier: asyncio.Barrier

    ui: UIState
    net: NetworkState

    is_debug: bool

    async def run(self) -> None:
        while True:
            await self.wakeup_event.wait()
            envelope, chunk = await self.get_chunk()
            if envelope is None:
                break
            if chunk is None:
                continue

            try:
                if chunk.current_pos > chunk.end:
                    await self.process_chunk(chunk)

                if not chunk.is_finished:
                    await log(
                        self.ui,
                        f"Truncated read for {chunk.file.actual_filename}. "
                        f"Requeuing remaining {chunk.remaining} bytes.",
                        status=LogStatus.WARNING,
                        throttle_key="truncated_read",
                        throttle_sec=2.0,
                    )
                    await self.requeue_chunk(envelope, chunk, delay_range=(0.1, 1.0))
                    continue
                await self.file_done(chunk)
            except Exception as e:
                await self.handle_worker_error(envelope, chunk, e)

    async def get_chunk(self) -> tuple[Envelope[Chunk] | None, Chunk | None]:
        envelope = await self.chunks_inbox.get()
        msg = envelope.payload

        match msg:
            case Chunk() as chunk:
                envelope = cast(Envelope[Chunk], envelope)

                file_obj = chunk.file
                if not file_obj or file_obj.is_failed:
                    return envelope, None

                return envelope, chunk
            case StopMsg():
                await self.controller_outbox.put(MaxLimitSignal())

                position = await self.barrier.wait()

                if position == 0:
                    await self._finally()

                return None, None

            case _:
                if self.is_debug:
                    raise RuntimeError(
                        f"Unknown message type in links_inbox: {type(msg)}"
                    )
                await log(
                    self.ui,
                    f"Received unknown message: {msg}",
                    status=LogStatus.ERROR,
                )
                return envelope, None

    @abstractmethod
    async def _finally(self) -> None:
        pass

    async def handle_worker_error(
        self, envelope: Envelope[Chunk], chunk: Chunk, e: Exception
    ) -> None:
        if isinstance(e, WorkerScaleDown):
            await self.chunks_inbox.put(envelope)
            return

        if isinstance(e, RequestsError):
            await self._handle_requests_error(envelope, chunk, e)
            self.dynamic_limit = max(self.dynamic_limit - 1, 1)
            await self.controller_outbox.put(NetworkCongestionSignal())
            return

        if isinstance(e, TimeoutError):
            await self.requeue_chunk(envelope, chunk)
            return

        tb_str = traceback.format_exc()

        if self.is_debug:
            await log(self.ui, f"CRITICAL CRASH:\n{tb_str}", status=LogStatus.CRITICAL)

        else:
            await log(
                self.ui,
                f"Worker internal crash: {e!r}",
                status=LogStatus.CRITICAL,
                traceback=tb_str,
            )
        raise e

    async def _handle_requests_error(
        self, envelope: Envelope[Chunk], chunk: Chunk, e: RequestsError
    ) -> None:
        """Разбирает сетевые ошибки и решает: убить файл или переповторить чанк."""
        response = get_error_response(e)
        if not isinstance(response, Response):
            await self.requeue_chunk(envelope, chunk)
            return

        status = response.status_code

        if status in {400, 401, 403, 404, 410, 416}:
            await log(
                self.ui,
                f"Chunk for {chunk.file.actual_filename} "
                f"failed permanently (HTTP {status}).",
                status=LogStatus.ERROR,
            )
            await self._handle_critical_requests_error(chunk, response)
        else:
            await self.requeue_chunk(envelope, chunk, delay_range=(0.5, 2.0))

    @abstractmethod
    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        pass

    @abstractmethod
    async def requeue_chunk(
        self,
        envelope: Envelope[Chunk],
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        pass

    @abstractmethod
    async def process_chunk(self, chunk: Chunk) -> None:
        pass

    @abstractmethod
    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:
        pass


@my_dataclass
class StreamDownloadWorker(BaseDownloadWorker):
    stream_chunks_outbox: asyncio.PriorityQueue[Envelope[StreamChunk | StopMsg]]
    file_discovery_outbox: asyncio.Queue[File | StopMsg]

    async def _finally(self) -> None:
        await self.file_discovery_outbox.put(StopMsg())

        self.all_complete.set()

    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        raise DownloadFailedError(
            url=chunk.file.meta.url,
            status_code=response.status_code,
            reason=response.reason,
        )

    async def requeue_chunk(
        self,
        envelope: Envelope[Chunk],
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        file_obj = chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            raise StreamError(
                url=chunk.file.meta.url, filename=chunk.file.actual_filename
            )
        await self.chunks_inbox.put(envelope)
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def process_chunk(self, chunk: Chunk) -> None:
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None
        buffer_list: list[bytes] = []
        current_buffer_size = 0

        async with stream_chunk(
            self.net,
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            try:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.put(RegisterStreamCmd(stream=r))
                bytes_to_read = chunk.end - chunk.current_pos + 1

                async for data in aiter_bytes(r, chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer_list.append(data)
                    current_buffer_size += len(data)

                    bytes_to_read -= len(data)
                    await self.state_outbox.put(
                        ProgressDeltaCmd(
                            file_id=chunk.file.meta.id, delta_bytes=len(data)
                        )
                    )

                    if not self.wakeup_event.is_set():
                        raise WorkerScaleDown

                    if bytes_to_read <= 0:
                        break

                if random.random() < 0.1:
                    await try_scale_up(self.net.rate_limiter)

            finally:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.put(RemoveStreamCmd(stream=r))

                if buffer_list:
                    await self.stream_chunks_outbox.put(
                        Envelope(
                            sort_key=(chunk.current_pos,),
                            payload=StreamChunk(
                                start=chunk.current_pos, data=buffer_list
                            ),
                        )
                    )
                    chunk.current_pos = chunk.current_pos + current_buffer_size

    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:
        pass


@my_dataclass
class DiskDownloadWorker(BaseDownloadWorker):
    disk_outbox: asyncio.Queue[WriteChunk | StopMsg]
    reg_events_outbox: asyncio.Queue[StateKeeperCmd]
    file_limit_outbox: asyncio.Queue[FileCompleted]

    fs: StorageBackend

    async def _finally(self) -> None:
        self.all_complete.set()
        await self.disk_outbox.put(StopMsg())

    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        chunk.file.is_failed = True
        self.fs.delete_file(chunk.file.actual_filename)

    async def requeue_chunk(
        self,
        envelope: Envelope[Chunk],
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        file_obj = chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            await log(
                self.ui,
                f"Connection dropped for {chunk.file.actual_filename}. "
                f"Server does not support resume. Restarting download from 0 bytes.",
                status=LogStatus.WARNING,
            )

            downloaded_so_far = chunk.current_pos - chunk.start
            if downloaded_so_far > 0:
                await self.state_outbox.put(
                    ProgressDeltaCmd(
                        file_id=chunk.file.meta.id, delta_bytes=-downloaded_so_far
                    )
                )

            chunk.current_pos = chunk.start

            fd = file_obj.fd
            if fd is not None:
                loop = asyncio.get_running_loop()
                # truncate(0) обрезает файл до 0 байт
                await loop.run_in_executor(None, os.ftruncate, fd, 0)

                # Если изначально размер был известен, снова выделяем место
                if file_obj.meta.content_length > 0:
                    await loop.run_in_executor(
                        None, os.ftruncate, fd, file_obj.meta.content_length
                    )
        await self.chunks_inbox.put(envelope)
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def process_chunk(self, chunk: Chunk) -> None:  # noqa
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None

        buffer_list: list[bytes] = []
        current_buffer_size = 0

        fd = chunk.file.fd

        if fd is None:
            fd = self.fs.open_file(chunk.file.actual_filename)
        buffer_size = 1_048_576
        async with stream_chunk(
            self.net,
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            try:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.put(RegisterStreamCmd(stream=r))
                bytes_to_read = chunk.end - chunk.current_pos + 1

                async for data in aiter_bytes(r, chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer_list.append(data)
                    current_buffer_size += len(data)

                    bytes_to_read -= len(data)
                    await self.state_outbox.put(
                        ProgressDeltaCmd(
                            file_id=chunk.file.meta.id, delta_bytes=len(data)
                        )
                    )

                    if current_buffer_size >= buffer_size:
                        await self.disk_outbox.put(
                            WriteChunk(
                                fd=fd,
                                offset=chunk.current_pos,
                                length=current_buffer_size,
                                data=buffer_list,
                            )
                        )
                        chunk.current_pos += current_buffer_size

                        buffer_list.clear()
                        current_buffer_size = 0
                        if random.random() < 0.1:
                            await try_scale_up(self.net.rate_limiter)

                    if bytes_to_read <= 0:
                        break

                    if not self.wakeup_event.is_set():
                        raise WorkerScaleDown

            finally:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.put(RemoveStreamCmd(stream=r))
                if buffer_list:
                    await self.disk_outbox.put(
                        WriteChunk(
                            fd=fd,
                            offset=chunk.current_pos,
                            length=current_buffer_size,
                            data=buffer_list,
                        )
                    )
                    chunk.current_pos += current_buffer_size

    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:

        filename = chunk.file.actual_filename
        file_obj = chunk.file
        if chunk.file.meta.content_length:
            if chunk.file.verified or not chunk.file.is_complete:
                return
            chunk.file.verified = True
            if not self.fs.verify_size(filename, file_obj.meta.content_length):
                return
        if file_obj.meta.expected_checksum:
            await log(
                self.ui,
                f"Verifying Hash checksum for {chunk.file.actual_filename}...",
                status=LogStatus.INFO,
            )
            await self.fs.verify_file_hash(
                file_obj.actual_filename,
                file_obj.meta.expected_checksum.value,
                file_obj.meta.expected_checksum.algorithm,
            )
            await log(
                self.ui,
                f"Integrity confirmed: {chunk.file.actual_filename}",
                status=LogStatus.SUCCESS,
            )
        self.fs.close_file(fd_or_conn=file_obj.fd)
        self.fs.delete_state(filename)
        await done(self.ui, file_obj.meta.id, filename)
        await self.reg_events_outbox.put(RemoveFileCmd(file_id=chunk.file.meta.id))
        await self.file_limit_outbox.put(FileCompleted())
