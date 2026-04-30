import asyncio
import errno
import os

from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.models import Envelope, UIState, WriteChunk, my_dataclass
from hydrastream.monitor import log


@my_dataclass
class DiskwWriter:
    disk_inbox: asyncio.Queue[Envelope[WriteChunk | None]]
    credit_outbox: asyncio.Queue[int]
    flush_event: asyncio.Event
    BUFFER_SIZE: int
    fs: StorageBackend
    ui: UIState

    async def run(self) -> None:
        batch_bytes: list[WriteChunk] = []
        current_batch_size = 0

        while True:
            envelope = await self.disk_inbox.get()

            if envelope.payload:
                batch_bytes.append(envelope.payload)
                current_batch_size += envelope.payload.length

            if batch_bytes and (
                current_batch_size >= self.BUFFER_SIZE or envelope.payload is None
            ):
                try:
                    await self.flush_to_disk(batch_bytes)
                    # Выдаем кредиты ТОЛЬКО ПОСЛЕ успешной записи
                    await self.credit_outbox.put(current_batch_size)

                    batch_bytes.clear()
                    current_batch_size = 0

                except Exception as e:
                    msg = self.handle_disk_error(e)
                    await log(
                        self.ui,
                        f"Disk Write Failure: {msg}",
                        status=LogStatus.CRITICAL,
                    )
                    raise RuntimeError(msg) from e

            if envelope.payload is None and not envelope.is_poison_pill:
                self.flush_event.set()

            if envelope.is_poison_pill:
                break

    def handle_disk_error(self, e: Exception) -> str:
        reason = "Unknown"
        if isinstance(e, OSError):
            sys_msg = os.strerror(e.errno) if e.errno else "Unknown"
            reasons = {
                errno.ENOSPC: f"STORAGE FULL: {sys_msg}. Action: Clean up disk space.",
                errno.EDQUOT: f"STORAGE FULL: {sys_msg}. Action: Clean up disk space.",
                errno.EIO: (
                    f"HARDWARE FAILURE: {sys_msg}. Action: Check drive SMART status."
                ),
                errno.EBADF: (
                    f"RUNTIME ERROR: {sys_msg}. Action: Check for file closing races."
                ),
            }
            if e.errno is not None:
                reason = reasons.get(e.errno, f"OS ERROR: {sys_msg} (code {e.errno})")

        return reason

    async def flush_to_disk(self, batch_bytes: list[WriteChunk]) -> None:
        if not batch_bytes:
            return

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

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_all_sync, coalesced)

    def _write_all_sync(self, coalesced: list[WriteChunk]) -> None:
        for chunk in coalesced:
            self.fs.write_chunk_data(chunk.fd, chunk.data, chunk.length, chunk.offset)
