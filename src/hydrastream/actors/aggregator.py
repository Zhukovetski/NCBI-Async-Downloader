import asyncio
from dataclasses import field

from hydrastream.actors.throttler import (
    DiskBufferClearedSignal,
    DiskBufferFullSignal,
    ThrottlerMsg,
)
from hydrastream.actors.writer import WriteCompleted
from hydrastream.exceptions import LogStatus
from hydrastream.models import StopMsg, UIState, WriteChunk, my_dataclass
from hydrastream.monitor import log


@my_dataclass
class FlushCmd:
    pass


@my_dataclass
class DiskAggregator:
    disk_inbox: asyncio.Queue[WriteChunk | FlushCmd | StopMsg]
    throttler_outbox: asyncio.Queue[ThrottlerMsg]
    ack_inbox: asyncio.Queue[WriteCompleted | StopMsg]
    writer_outbox: asyncio.Queue[list[WriteChunk] | StopMsg]
    flush_event: asyncio.Event
    MAX_BUFFER: int

    ui: UIState
    is_debug: bool

    current_buffer: list[WriteChunk] = field(default_factory=list[WriteChunk])
    current_size: int = 0
    is_writing_now: bool = False

    async def run(self) -> None:

        while True:
            msg = await self.disk_inbox.get()

            match msg:
                case WriteChunk():
                    self.current_buffer.append(msg)
                    self.current_size += msg.length

                    if self.current_size >= self.MAX_BUFFER:
                        await self._persist_buffer()

                case FlushCmd():
                    await self._persist_buffer()

                    self.flush_event.set()

                case StopMsg():
                    await self._persist_buffer()

                    await self.writer_outbox.put(StopMsg())
                    break

                case _:
                    if self.is_debug:
                        raise RuntimeError(
                            f"Unknown message type in disk_inbox: {type(msg)}"
                        )
                    await log(
                        self.ui,
                        f"Received unknown message: {msg}",
                        status=LogStatus.ERROR,
                    )

    async def _persist_buffer(self) -> None:

        if self.is_writing_now:
            await self.throttler_outbox.put(DiskBufferFullSignal())

            try:
                async with asyncio.timeout(60.0):
                    await self.ack_inbox.get()

            except TimeoutError as e:
                raise RuntimeError(
                    "DiskWriter stopped responding! Hardware failure?"
                ) from e

            self.is_writing_now = False
            await self.throttler_outbox.put(DiskBufferClearedSignal())

        if self.current_buffer:
            batch = await self._coalesce(self.current_buffer)
            await self.writer_outbox.put(batch)

            self.is_writing_now = True
            self.current_buffer.clear()
            self.current_size = 0

    async def _coalesce(self, batch_bytes: list[WriteChunk]) -> list[WriteChunk]:

        batch_bytes.sort()

        coalesced: list[WriteChunk] = []
        curr = batch_bytes[0]

        acc_data_chunks: list[bytes] = curr.data
        acc_len = curr.length

        for next_chunk in batch_bytes[1:]:
            if (
                curr.fd == next_chunk.fd
                and (curr.offset + acc_len) == next_chunk.offset
            ):
                acc_data_chunks.extend(next_chunk.data)
                acc_len += next_chunk.length
            else:
                coalesced.append(
                    WriteChunk(
                        fd=curr.fd,
                        offset=curr.offset,
                        length=acc_len,
                        data=acc_data_chunks,
                    )
                )
                curr = next_chunk
                acc_data_chunks = curr.data
                acc_len = curr.length

        coalesced.append(
            WriteChunk(
                fd=curr.fd,
                offset=curr.offset,
                length=acc_len,
                data=acc_data_chunks,
            )
        )
        return coalesced
