import asyncio
import errno
import os

from hydrastream.exceptions import LogStatus
from hydrastream.models import WriteChunk, my_dataclass
from hydrastream.monitor import log


@my_dataclass
class DiskwWriter:
    async def disk_writer(self) -> None:
        batch_bytes: list[WriteChunk] = []
        current_batch_size = 0

        while True:
            envelope = await self.queues.disk.get()

            if envelope.payload:
                batch_bytes.append(envelope.payload)
                current_batch_size += envelope.payload.length

            if batch_bytes and (
                current_batch_size >= self.config.BUFFER_SIZE
                or envelope.payload is None
            ):
                try:
                    await self.flush_to_disk(self, batch_bytes)

                    batch_bytes.clear()
                    current_batch_size = 0

                except Exception as e:
                    msg = self.handle_disk_error(e)
                    await log(
                        self.ui,
                        f"Disk Write Failure: {e}",
                        status=LogStatus.CRITICAL,
                    )
                    raise RuntimeError(msg) from e

            if envelope:
                self.sync.flush_event.set()

            if envelope.is_poison_pill:
                break

    def handle_disk_error(e: Exception) -> str:
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
        acc_data = list(curr.data)
        acc_len = curr.length

        for next_chunk in batch_bytes[1:]:
            # Проверяем, можно ли приклеить следующий чанк к текущему
            if (
                curr.fd == next_chunk.fd
                and (curr.offset + acc_len) == next_chunk.offset
            ):
                acc_data.extend(next_chunk.data)
                acc_len += next_chunk.length
            else:
                # Сохраняем накопленный результат и переходим к новому
                coalesced.append(
                    WriteChunk(
                        fd=curr.fd, offset=curr.offset, length=acc_len, data=acc_data
                    )
                )
                curr = next_chunk
                acc_data = list(curr.data)
                acc_len = curr.length

        # Не забываем добавить последний накопленный чанк
        coalesced.append(
            WriteChunk(fd=curr.fd, offset=curr.offset, length=acc_len, data=acc_data)
        )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_all_sync, coalesced)

    def _write_all_sync(self, coalesced: list[WriteChunk]) -> None:
        for chunk in coalesced:
            self.fs.write_chunk_data(chunk.fd, chunk.data, chunk.length, chunk.offset)
