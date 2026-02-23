from rich.console import Console
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)


class ProgressMonitor:
    def __init__(self):
        # Настраиваем колонки: Имя, Бар, % , Размер, Скорость, Время
        self.progress = Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
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
            transient=True,
        )

        # Словарь для связи имени файла и ID задачи в Rich
        # { "E_coli.gz": TaskID(1), "Human.gz": TaskID(2) }
        self.tasks = {}
        self.console = Console()

    def log(self, content):
        self.progress.log(content)

    def consol(self, content):
        self.progress.console.print(content)

    def add_file(self, filename, total_size):
        """Регистрирует новый файл в UI"""
        if filename not in self.tasks:
            task_id = self.progress.add_task(
                "download", filename=filename, total=total_size, start=True
            )

            self.tasks[filename] = task_id

    def update(self, filename, advance_bytes):
        """
        Главный метод: обновляет прогресс конкретного файла.
        advance_bytes - сколько байт ТОЛЬКО ЧТО скачали (размер чанка)
        """
        if filename in self.tasks:
            task_id = self.tasks[filename]
            self.progress.update(task_id, advance=advance_bytes)

    def done(self, filename):
        """Можно перекрасить бар в зеленый, когда готово"""
        if filename in self.tasks:
            # Например, убираем скорость и пишем DONE
            pass

    def start(self):
        self.progress.start()

    def stop(self):
        self.progress.stop

    # Контекстный менеджер, чтобы всё красиво открылось и закрылось
    def __enter__(self):
        self.progress.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()
