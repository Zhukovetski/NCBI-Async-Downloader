# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio

from hydrastream.engine import send_poison_pills
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.models import (
    Chunk,
    Envelope,
    File,
    HydraContext,
    UIState,
    my_dataclass,
)
from hydrastream.monitor import log, update_filename


@my_dataclass(frozen=True)
class FileCompleted:
    pass


@my_dataclass
class FileDispatcher:
    is_stream: bool
    limit: int
    current_files: int = 0
    files_q: asyncio.PriorityQueue[Envelope[File | None]]
    file_limit_q: asyncio.Queue[object]
    chunks_q: asyncio.PriorityQueue[Envelope[Chunk | None]]
    num_memory_throtllers: int
    fs: StorageBackend
    ui: UIState

    async def run(self, ctx: HydraContext) -> None:  # noqa: C901
        pending_file: Envelope[File | None] | None = None
        loop = asyncio.get_running_loop()

        while True:
            if pending_file is None:
                pending_file = await self.files_q.get()

                if pending_file.is_poison_pill:
                    await send_poison_pills(self.chunks_q, self.num_memory_throtllers)
                    break

                if not (file_obj := pending_file.payload):
                    continue

                if ctx.stream:
                    await ctx.queues.file_discovery.put(file_obj.meta.id)

                if self.current_files >= self.limit:
                    await self.file_limit_q.get()
                    self.current_files -= 1
                    continue

                self.current_files += 1
                if not self.is_stream:
                    await loop.run_in_executor(
                        None, self._prepare_file_on_disk, file_obj
                    )

                if file_obj.meta.original_filename != file_obj.actual_filename:
                    await log(
                        self.ui,
                        f"{file_obj.meta.original_filename} already exists. "
                        f"Saving as {file_obj.actual_filename}.",
                        status=LogStatus.WARNING,
                    )

                file_obj.create_chunks()
                for c in file_obj.chunks:
                    if c.current_pos <= c.end:
                        await self.chunks_q.put(
                            Envelope(
                                sort_key=(c.current_pos, file_obj.meta.id),
                                payload=c,
                            )
                        )
                pending_file = None
                file_obj = None

    def _prepare_file_on_disk(self, file_obj: File) -> None:
        new_filename = self.fs.allocate_space(
            filename=file_obj.meta.original_filename, size=file_obj.meta.content_length
        )
        if new_filename:
            file_obj.actual_filename = new_filename
            update_filename(self.ui, file_obj.meta.original_filename, new_filename)
        else:
            file_obj.actual_filename = file_obj.meta.original_filename

        file_obj.fd = self.fs.open_file(filename=file_obj.actual_filename)
