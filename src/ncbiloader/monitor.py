# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from types import TracebackType
from typing import Self

from rich.console import Console
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)


class ProgressMonitor:
    def __init__(self) -> None:
        # Настраиваем колонки: Имя, Бар, % , Размер, Скорость, Время
        self.progress = Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None, complete_style="red", finished_style="bold green"),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            FileSizeColumn(),  # Сколько скачано
            "/",
            TotalFileSizeColumn(),  # Сколько всего
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            expand=True,
            refresh_per_second=10,
        )

        # Словарь для связи имени файла и ID задачи в Rich
        # { "E_coli.gz": TaskID(1), "Human.gz": TaskID(2) }
        self.tasks: dict[str, TaskID] = {}
        self.console = Console()

    def log(self, content: str) -> None:
        self.progress.log(content)

    def consol(self, content: str) -> None:
        self.progress.console.print(content)

    def add_file(self, filename: str, total_size: int) -> None:
        """Регистрирует новый файл в UI"""
        if filename not in self.tasks:
            task_id = self.progress.add_task("download", filename=filename, total=total_size, start=True)

            self.tasks[filename] = task_id

    def update(self, filename: str, advance_bytes: int) -> None:
        """
        Главный метод: обновляет прогресс конкретного файла.
        advance_bytes - сколько байт ТОЛЬКО ЧТО скачали (размер чанка)
        """
        if filename in self.tasks:
            task_id = self.tasks[filename]
            self.progress.update(task_id, advance=advance_bytes)

    def done(self, filename: str) -> None:
        if filename in self.tasks:
            task_id = self.tasks[filename]

            # 1. Меняем описание (добавляем галочку)
            self.progress.update(
                task_id,
                description=f"[bold green]✔ {filename}",
                completed=self.progress.tasks[task_id].total,  # Принудительно 100%
                style="green",  # Красим саму полоску
            )

    def start(self) -> None:
        self.progress.start()

    def stop(self) -> None:
        self.progress.stop()

    def __enter__(self) -> Self:
        self.progress.start()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:

        self.progress.stop()
