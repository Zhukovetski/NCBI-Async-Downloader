# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import os
import random
import sys
from dataclasses import dataclass

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    WorkerScaleDown,
)
from hydrastream.models import Chunk, HydraContext
from hydrastream.monitor import done, log, update
from hydrastream.network import stream_chunk, try_scale_up


@dataclass
class DownloadWorker:
    worker_id: int

    async def file_done(self, ctx: HydraContext, chunk: Chunk) -> None:
        if ctx.stream:
            return

        filename = chunk.file.meta.filename
        file_obj = chunk.file
        if chunk.file.meta.content_length:
            if chunk.file.verified or not chunk.file.is_complete:
                return
            chunk.file.verified = True
            if not ctx.fs.verify_size(filename, file_obj.meta.content_length):
                return
        if file_obj.meta.expected_checksum:
            await log(
                ctx.ui,
                f"Verifying Hash checksum for {chunk.file.meta.filename}...",
                status=LogStatus.INFO,
            )
            await ctx.fs.verify_file_hash(
                file_obj.meta.filename,
                file_obj.meta.expected_checksum.value,
                file_obj.meta.expected_checksum.algorithm,
            )
            await log(
                ctx.ui,
                f"Integrity confirmed: {chunk.file.meta.filename}",
                status=LogStatus.SUCCESS,
            )
        ctx.fs.close_file(fd_or_conn=file_obj.fd)
        ctx.fs.delete_state(filename)
        await done(ctx.ui, filename)
        del ctx.files[chunk.file.meta.id]
        ctx.current_files_id.remove(chunk.file.meta.id)
        async with ctx.sync.current_files:
            ctx.sync.current_files.notify_all()

    async def get_chunk(self, ctx: HydraContext) -> Chunk | None:
        if ctx.stream:
            id, chunk = await ctx.queues.chunk.get()
        else:
            chunk, id = await ctx.queues.chunk.get()

        if not isinstance(chunk, Chunk) or not isinstance(id, int):
            raise ValueError

        if sys.maxsize - ctx.tasks.workers < id:
            if not ctx.sync.stop_adaptive_controller.is_set():
                ctx.sync.stop_adaptive_controller.set()
                ctx.ui.speed.controller_checkpoint_event.set()
                ctx.dynamic_limit = sys.maxsize
                async with ctx.sync.dynamic_limit:
                    ctx.sync.dynamic_limit.notify_all()

            if id == sys.maxsize:
                if ctx.stream:
                    await ctx.queues.file_discovery.put(-1)

                ctx.sync.all_complete.set()
                ctx.ui.speed.throttler_checkpoint_event.set()

            raise asyncio.CancelledError

        file_obj = chunk.file
        if not file_obj or file_obj.is_failed:
            return None
        if ctx.stream:
            async with ctx.sync.chunk_from_future:
                await ctx.sync.chunk_from_future.wait_for(
                    lambda: (
                        ctx.next_offset + ctx.config.STREAM_BUFFER_SIZE
                        >= chunk.current_pos
                    )
                )
        return chunk

    async def run(self, ctx: HydraContext) -> None:
        chunk = None
        while True:
            try:
                if self.worker_id >= ctx.dynamic_limit:
                    async with ctx.sync.dynamic_limit:
                        await ctx.sync.dynamic_limit.wait_for(
                            lambda: self.worker_id < ctx.dynamic_limit
                        )
                chunk = await self.get_chunk(ctx)
                if chunk is None:
                    continue
                await self.process_chunk(ctx, chunk)
                await self.file_done(ctx, chunk)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_worker_error(ctx, e, chunk)

    async def handle_worker_error(
        self, ctx: HydraContext, e: Exception, chunk: Chunk | None
    ) -> None:
        if isinstance(e, WorkerScaleDown):
            if chunk:
                await ctx.queues.chunk.put((-1, chunk) if ctx.stream else (chunk, -1))
            return

        if isinstance(e, RequestsError):
            await self._handle_requests_error(ctx, e, chunk)
            ctx.dynamic_limit = max(ctx.dynamic_limit - 1, 1)
            async with ctx.sync.dynamic_limit:
                ctx.sync.dynamic_limit.notify_all()
            return

        if isinstance(e, TimeoutError):
            if chunk:
                await self.requeue_chunk(ctx, chunk)
            return

        await log(ctx.ui, f"Worker internal crash: {e!r}", status=LogStatus.CRITICAL)
        raise e

    async def _handle_requests_error(
        self, ctx: HydraContext, e: RequestsError, chunk: Chunk | None
    ) -> None:
        """Разбирает сетевые ошибки и решает: убить файл или переповторить чанк."""
        if not chunk:
            return

        response = e.response
        if not isinstance(response, Response):
            await self.requeue_chunk(ctx, chunk)
            return

        status = response.status_code

        # Логика "Фатальных" ошибок
        if status in {400, 401, 403, 404, 410, 416}:
            await log(
                ctx.ui,
                f"Chunk for {chunk.file.meta.filename} "
                f"failed permanently (HTTP {status}).",
                status=LogStatus.ERROR,
            )
            chunk.file.is_failed = True
            ctx.fs.delete_file(chunk.file.meta.filename)

            if ctx.stream:
                raise DownloadFailedError(
                    url=chunk.file.meta.url,
                    status_code=status,
                    reason=response.reason,
                )
        else:
            await self.requeue_chunk(ctx, chunk, delay_range=(0.5, 2.0))

    async def requeue_chunk(
        self,
        ctx: HydraContext,
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        if not chunk:
            return

        file_obj = chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            if ctx.stream:
                err_msg = (
                    f"Stream interrupted for {chunk.file.meta.filename}. "
                    f"Server does not support partial downloads (Range requests). "
                    f"Cannot resume stream. Aborting."
                )
                await log(ctx.ui, err_msg, status=LogStatus.CRITICAL)

                file_obj.is_failed = True
                return

            await log(
                ctx.ui,
                f"Connection dropped for {chunk.file.meta.filename}. "
                f"Server does not support resume. Restarting download from 0 bytes.",
                status=LogStatus.WARNING,
            )

            downloaded_so_far = chunk.current_pos - chunk.start
            if downloaded_so_far > 0:
                update(ctx.ui, chunk.file.meta.filename, -downloaded_so_far)

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
        if ctx.stream:
            await ctx.queues.chunk.put((-1, chunk))
        else:
            await ctx.queues.chunk.put((chunk, -1))
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def disk_process_chunk(
        self,
        ctx: HydraContext,
        chunk: Chunk,
        headers: dict[str, str] | None,
    ) -> None:
        buffer = bytearray()
        fd = chunk.file.fd

        if fd is None:
            fd = ctx.fs.open_file(chunk.file.meta.filename)
        buffer_size = 1_048_576
        async with stream_chunk(
            ctx.net,
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            ctx.active_stream.add(r)
            try:
                async for data in r.aiter_content(chunk_size=131072):
                    buffer.extend(data)
                    update(ctx.ui, chunk.file.meta.filename, len(data))
                    if len(buffer) >= buffer_size:
                        await ctx.fs.write_chunk_data(fd, buffer, chunk.current_pos)
                        chunk.current_pos += len(buffer)
                        buffer = bytearray()
                        if random.random() < 0.1:
                            await try_scale_up(ctx.net.rate_limiter)
                    if ctx.dynamic_limit <= self.worker_id:
                        raise WorkerScaleDown

            finally:
                ctx.active_stream.remove(r)
                if buffer:
                    await ctx.fs.write_chunk_data(fd, buffer, chunk.current_pos)
                    chunk.current_pos += len(buffer)

    async def stream_process_chunk(
        self,
        ctx: HydraContext,
        chunk: Chunk,
        headers: dict[str, str] | None,
    ) -> None:
        buffer = bytearray()
        async with stream_chunk(
            ctx.net,
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            ctx.active_stream.add(r)
            try:
                async for data in r.aiter_content(chunk_size=131072):
                    buffer.extend(data)
                    update(ctx.ui, chunk.file.meta.filename, len(data))
                    if len(buffer) > ctx.config.STREAM_CHUNK_SIZE:
                        await ctx.queues.stream.put((chunk.current_pos, buffer))
                        chunk.current_pos = chunk.current_pos + len(buffer)
                        buffer = bytearray()
                    if ctx.dynamic_limit <= self.worker_id:
                        raise WorkerScaleDown
                await ctx.queues.stream.put((chunk.current_pos, buffer))
                chunk.current_pos = chunk.current_pos + len(buffer)
                buffer = bytearray()
            except asyncio.CancelledError:
                raise
            except Exception:
                if ctx.stream and buffer:
                    with contextlib.suppress(asyncio.QueueFull):
                        ctx.queues.stream.put_nowait((chunk.current_pos, buffer))
                        chunk.current_pos = chunk.current_pos + len(buffer)
                raise
            finally:
                ctx.active_stream.remove(r)

    async def process_chunk(self, ctx: HydraContext, chunk: Chunk) -> None:
        if chunk.current_pos > chunk.end:
            return
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None
        if not ctx.stream:
            await self.disk_process_chunk(ctx, chunk, headers)
        else:
            await self.stream_process_chunk(ctx, chunk, headers)
