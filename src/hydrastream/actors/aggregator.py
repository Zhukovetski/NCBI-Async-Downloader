import asyncio

from hydrastream.actors.throttler import DiskBufferClearedSignal, DiskBufferFullSignal
from hydrastream.models import Cmd, WriteChunk, my_dataclass


@my_dataclass
class DiskAggregator:
    disk_inbox: asyncio.Queue[WriteChunk | Cmd]
    throttler_outbox: asyncio.Queue[object]
    ack_inbox: asyncio.Queue[object]
    writer_outbox: asyncio.Queue[list[WriteChunk] | None]
    flush_event: asyncio.Event
    MAX_BUFFER: int

    async def run(self) -> None:
        current_buffer: list[WriteChunk] = []
        current_size = 0
        is_writing_now = False

        while True:
            msg = await self.disk_inbox.get()

            is_control_msg = isinstance(msg, Cmd)
            is_poison_pill = is_control_msg and msg.is_poison_pill

            if isinstance(msg, WriteChunk):
                current_buffer.append(msg)
                current_size += msg.length

            needs_flush = (current_size >= self.MAX_BUFFER) or is_control_msg

            if needs_flush:
                if is_writing_now:
                    await self.throttler_outbox.put(DiskBufferFullSignal())

                    await self.ack_inbox.get()
                    is_writing_now = False

                    await self.throttler_outbox.put(DiskBufferClearedSignal())

                if current_buffer:
                    coalesced_batch = await self._coalesce(current_buffer)
                    await self.writer_outbox.put(coalesced_batch)

                    is_writing_now = True
                    current_buffer.clear()
                    current_size = 0

                if is_control_msg:
                    if is_poison_pill:
                        await self.writer_outbox.put(None)
                        break

                    self.flush_event.set()

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
