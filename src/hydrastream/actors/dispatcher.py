# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from abc import ABC, abstractmethod

from hydrastream.engine import send_poison_pills
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.models import (
    Chunk,
    Envelope,
    File,
    UIState,
    my_dataclass,
)
from hydrastream.monitor import log, update_filename


@my_dataclass(frozen=True)
class FileCompleted:
    pass


@my_dataclass
class BaseFileDispatcher(ABC):
    limit: int
    current_files: int = 0
    files_inbox: asyncio.PriorityQueue[Envelope[File | None]]
    file_limit_inbox: asyncio.Queue[FileCompleted]
    chunks_outbox: asyncio.PriorityQueue[Envelope[Chunk | None]]
    num_memory_throtllers: int
    ui: UIState

    async def run(self) -> None:
        pending_file: Envelope[File | None] | None = None

        while True:
            if pending_file is None:
                pending_file = await self.files_inbox.get()

                if pending_file.is_poison_pill:
                    await send_poison_pills(
                        self.chunks_outbox, self.num_memory_throtllers
                    )
                    break

                if not (file_obj := pending_file.payload):
                    continue

                if self.current_files >= self.limit:
                    await self.file_limit_inbox.get()
                    self.current_files -= 1
                    continue

                self.current_files += 1

                # 1. ВЫЗЫВАЕМ СПЕЦИФИЧНУЮ ПОДГОТОВКУ
                await self._prepare_file(file_obj)

                # 2. ОБЩАЯ ЛОГИКА ДЛЯ ВСЕХ (Пишем лог, если имя поменялось)
                if file_obj.meta.original_filename != file_obj.actual_filename:
                    await log(
                        self.ui,
                        f"{file_obj.meta.original_filename} already exists. "
                        f"Saving as {file_obj.actual_filename}.",
                        status=LogStatus.WARNING,
                    )

                # 3. Создаем чанки
                file_obj.create_chunks()

                for c in file_obj.chunks:
                    if c.current_pos <= c.end:
                        await self.chunks_outbox.put(
                            Envelope(
                                # 4. ВЫЗЫВАЕМ СПЕЦИФИЧНУЮ СОРТИРОВКУ
                                sort_key=self._get_sort_key(
                                    file_obj.meta.id, c.current_pos
                                ),
                                payload=c,
                            )
                        )
                pending_file = None

    @abstractmethod
    async def _prepare_file(self, file_obj: File) -> None:
        """Специфичная логика подготовки файла"""
        pass

    @abstractmethod
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        """Специфичный ключ сортировки для очередей"""
        pass


@my_dataclass
class StreamFileDispatcher(BaseFileDispatcher):
    file_discovery: asyncio.Queue[File | None]

    async def _prepare_file(self, file_obj: File) -> None:
        # Для стрима просто закидываем файл в трубу (имя не меняется)
        await self.file_discovery.put(file_obj)

    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # СТРИМ: Сначала ID файла, потом позиция (Качаем файлы по очереди!)
        return (file_id, current_pos)


@my_dataclass
class DiskFileDispatcher(BaseFileDispatcher):
    fs: StorageBackend

    async def _prepare_file(self, file_obj: File) -> None:

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._prepare_file_on_disk, file_obj)

    def _prepare_file_on_disk(self, file_obj: File) -> None:
        new_filename = self.fs.allocate_space(
            filename=file_obj.meta.original_filename, size=file_obj.meta.content_length
        )
        if new_filename:
            file_obj.actual_filename = new_filename
            update_filename(self.ui, file_obj.meta.id, new_filename)
        else:
            file_obj.actual_filename = file_obj.meta.original_filename

        file_obj.fd = self.fs.open_file(filename=file_obj.actual_filename)

    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # ДИСК: Сначала позиция, потом ID файла (Round-Robin параллельность!)
        return (current_pos, file_id)
