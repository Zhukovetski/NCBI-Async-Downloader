# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
from abc import ABC, abstractmethod

from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.models import (
    Chunk,
    Envelope,
    File,
    StopMsg,
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

    files_inbox: asyncio.PriorityQueue[Envelope[File | StopMsg]]
    chunks_outbox: asyncio.PriorityQueue[Envelope[Chunk | StopMsg]]
    file_limit_inbox: asyncio.Queue[FileCompleted]

    ui: UIState

    is_debug: bool

    async def run(self) -> None:
        pending_file: Envelope[File | StopMsg] | None = None

        while True:
            if pending_file is None:
                envelope = await self.files_inbox.get()
                msg = envelope.payload

                match msg:
                    case File() as file_obj:
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

                    case StopMsg():
                        await self.chunks_outbox.put(
                            Envelope(sort_key=(sys.maxsize,), payload=StopMsg())
                        )
                        break

                    case _:
                        if self.is_debug:
                            raise RuntimeError(
                                f"Unknown message type in files_inbox: {type(msg)}"
                            )
                        await log(
                            self.ui,
                            f"Received unknown message: {msg}",
                            status=LogStatus.ERROR,
                        )

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
