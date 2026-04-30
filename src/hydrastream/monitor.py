# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import shutil
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)
from rich.progress_bar import ProgressBar
from rich.rule import Rule
from rich.table import Column, Table
from rich.text import Text

from hydrastream.exceptions import HydraError, LogFileError, LogStatus
from hydrastream.models import File, UIState
from hydrastream.utils import format_size


def truncate_filename(name: str, w: int = 30) -> str:
    return f"{name[: w // 2 - 1]}...{name[-w // 2 + 2 :]}" if len(name) > w else name


def get_gradient_color(percentage: float) -> str:
    p = max(0, min(100, percentage or 0))
    if p < 50:
        r, g, b = 255, int((p / 50) * 255), 0
    else:
        r, g, b = int(255 - ((p - 50) / 50) * 255), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"


class GradientBar(BarColumn):
    def render(self, task: Task) -> ProgressBar:
        if task.total is None:
            self.complete_style = "cyan"
        elif task.finished:
            self.complete_style = "bold bright_green blink"
        else:
            self.complete_style = get_gradient_color(task.percentage)
        return super().render(task)


class GradientPercent(ProgressColumn):
    def render(self, task: Task) -> Text:
        if task.total is None:
            return Text(" CALC ", style="yellow")
        p = task.percentage
        color = get_gradient_color(p)
        return Text(f"{p:>5.1f}%", style=f"bold {color}")


async def date_print(ctx: UIState) -> None:
    current_date = datetime.now().strftime("%Y-%m-%d")
    date_header = f"[bold cyan] Date: {current_date}[/]"

    if not (ctx.display.no_ui or ctx.display.quiet):
        ctx.rich.console.print(Rule(date_header))
    await log(ctx, f"--- {current_date} ---")


async def report(ctx: UIState, error: HydraError, **log_extra: Any) -> None:  # noqa: ANN401
    """
    Передаем данные ошибки в логгер.
    asdict() превратит поля датакласса в ключи для JSON-лога.
    """
    # Убираем системные поля, которые не нужны в JSON-атрибутах
    data = asdict(error)
    for key in ["exit_code", "log_status", "message_tpl", "formatted_msg"]:
        data.pop(key, None)

    await log(
        ctx,
        f"[{error.error_id}] {error.formatted_msg}",
        status=error.log_status,
        **data,  # Все поля (filename, required и т.д.) попадут в JSON!
        **log_extra,  # Дополнительные флаги типа throttle_key
    )


def formatting_log(
    message: str | Rule, formatted_msg: str, status: LogStatus = LogStatus.INFO
) -> Panel | str | Rule:
    match status.upper():
        case "CRITICAL" | "INTERRUPT":
            renderable = Panel(
                f"[bold red]{message}[/]\n[dim white]Partial data may have been saved.",
                title="[bold red]Interrupted",
                border_style="red",
                expand=False,
            )
        case "ERROR":
            renderable = Panel(
                f"[bold red]{message}[/]",
                title="Error",
                border_style="red",
                padding=(0, 1),
            )
        case "WARNING":
            renderable = f"[yellow]{formatted_msg}[/]"
        case "INFO":
            renderable = f"[white]{formatted_msg}[/]"
        case "SUCCESS":
            renderable = f"[green]{formatted_msg}[/]"
        case _:
            renderable = message
    return renderable


async def log(
    ctx: UIState,
    message: str | Rule,
    *,
    status: LogStatus = LogStatus.INFO,
    progress: bool = False,
    throttle_key: str | None = None,
    throttle_sec: float = 10.0,
    **kwargs: object,
) -> None:
    if throttle_key:
        now = time.monotonic()
        last_time = ctx.log.log_throttle.get(throttle_key, 0.0)
        if now - last_time < throttle_sec:
            return
        ctx.log.log_throttle[throttle_key] = now
    if ctx.display.json_logs:
        # Собираем словарь для JSON
        log_record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": status.upper(),
            "message": message,
            **kwargs,  # Распаковываем дополнительные данные!
        }
        # Сериализуем в байты, потом в строку
        final_msg = orjson.dumps(log_record).decode("utf-8")
    else:
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        final_msg = f"{timestamp} {message}"

    if ctx.log.log_file:
        clean_msg = Text.from_markup(str(final_msg)).plain
        ctx.log.log_queue.put_nowait(clean_msg)

    if ctx.display.quiet:
        return

    if ctx.display.json_logs:
        sys.stdout.write(final_msg + "\n")
        return

    renderable = final_msg
    if ctx.rich.progress:
        renderable = formatting_log(message, renderable, status)
        if progress or status in ["WARNING", "ERROR", "CRITICAL", "INTERRUPT"]:
            ctx.rich.progress.console.print(renderable)
    else:
        ctx.rich.console.print(Text.from_markup(str(renderable)).plain)


async def log_worker(ctx: UIState) -> None:
    """
    Фоновый воркер. Живет всё время работы программы.
    Берет строки из очереди и пишет в ОТКРЫТЫЙ файл.
    """
    if not ctx.log.log_fd:
        return

    while True:
        msg = await ctx.log.log_queue.get()

        # Ядовитая пилюля для остановки логгера
        if msg is None:
            break

        try:
            ctx.log.log_fd.write(f"{msg}\n")
            ctx.log.log_fd.flush()  # Гарантируем, что строка сразу упала на диск
        except OSError as e:
            if ctx.display.debug:
                raise
            err = LogFileError(path=str(ctx.log.log_file), original_err=str(e))
            await log(ctx, f"{err.formatted_msg}", status=LogStatus.WARNING)

            ctx.log.log_fd.close()
            ctx.log.log_fd = None
            break


def add_file(ctx: UIState, filename: str, total_size: int | None = None) -> None:
    if total_size is not None:
        ctx.progress.total_bytes += total_size
        ctx.progress.total_files += 1

    if ctx.rich.progress:
        t_filename = truncate_filename(filename)
        if total_size is None:
            task_id = ctx.rich.progress.add_task(
                "Download Hash for", filename=t_filename, total=total_size
            )
        else:
            task_id = ctx.rich.progress.add_task(
                "Download file", filename=t_filename, total=total_size, visible=False
            )
        ctx.rich.tasks[filename] = task_id
        update_panel_title(ctx)


def update(ctx: UIState, filename: str, advance_bytes: int) -> None:
    ctx.rich.buffer[filename] += advance_bytes
    ctx.progress.download_bytes += advance_bytes

    if ctx.progress.download_bytes - ctx.speed.prev_bytes >= ctx.speed.bytes_to_check:
        ctx.speed.prev_bytes += ctx.speed.bytes_to_check

        ctx.speed.controller_checkpoint_event.set()
        ctx.speed.throttler_checkpoint_event.set()


def update_filename(ctx: UIState, old_filename: str, new_filename: str) -> None:
    if ctx.rich.progress:
        ctx.rich.progress.update(ctx.rich.tasks[old_filename], description=new_filename)
        ctx.rich.tasks[new_filename] = ctx.rich.tasks.pop(old_filename)


async def ui_refresh_actor(ctx: UIState, state_keeper_q: asyncio.Queue) -> None:
    """Независимый цикл отрисовки (Render Loop)"""
    reply_q = asyncio.Queue(maxsize=1)

    if not ctx.rich.progress:
        return

    while ctx.is_running:
        try:
            # 1. Засыпаем до следующего "кадра" (например, на 0.1 сек)
            await asyncio.sleep(ctx.rich.renewal_rate)

            # 2. Запрашиваем дельты у базы данных (StateKeeper)
            await state_keeper_q.put(GetUIDeltasCmd(reply_to=reply_q))
            deltas = await reply_q.get()

            # 3. Отрисовываем!
            for file_id, bytes_to_advance in deltas.items():
                if bytes_to_advance > 0 and file_id in ctx.rich.tasks:
                    ctx.rich.progress.update(
                        ctx.rich.tasks[file_id],
                        advance=bytes_to_advance,
                        visible=True,
                    )

                    if file_id not in ctx.rich.active_files:
                        ctx.rich.active_files.add(file_id)
                        update_panel_title(ctx)

        except Exception as e:
            if ctx.display.debug:
                raise
            await log(ctx, f"UI Refresh Error: {e!r}", status=LogStatus.ERROR)


def update_panel_title(ctx: UIState) -> None:
    if not ctx.rich.live:
        ctx.rich.dynamic_title = ""

    active = len(ctx.rich.active_files)
    ctx.rich.dynamic_title = (
        f"[bold white][green]{ctx.progress.files_completed}[/]/"
        + f"[blue]{ctx.progress.total_files}[/] Files | [yellow]{active} Active[/]"
    )


async def done(ctx: UIState, filename: str) -> None:
    if ctx.rich.progress and filename in ctx.rich.tasks:
        task_id = ctx.rich.tasks[filename]
        ctx.rich.progress.update(
            task_id, completed=ctx.rich.progress.tasks[task_id].total, visible=False
        )
        del ctx.rich.tasks[filename]
        ctx.rich.active_files.discard(filename)

        if ctx.rich.progress.tasks[task_id].total is not None:
            ctx.progress.files_completed += 1
            update_panel_title(ctx)
            await log(ctx, f"Done: {filename}", status=LogStatus.SUCCESS, progress=True)

    elif ctx.rich.buffer.get(filename, 0):
        ctx.progress.files_completed += 1
        await log(ctx, f"Done: {filename}", status=LogStatus.SUCCESS, progress=True)


def make_panel(ctx: UIState) -> Panel | str:
    if not ctx.rich.progress:
        return ""

    if not ctx.rich.tasks and ctx.is_running:
        return ""

    elapsed = time.monotonic() - ctx.progress.start_time
    avg_speed = ctx.progress.download_bytes / elapsed if elapsed > 0 else 0
    speed_str = f"{format_size(avg_speed)}/s"

    mins, secs = divmod(int(elapsed), 60)
    hours = 0
    if mins >= 60:
        hours, mins = divmod(mins, 60)
    time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

    remain_time = (
        (ctx.progress.total_bytes - ctx.progress.download_bytes) / avg_speed
        if ctx.progress.total_bytes and avg_speed
        else 0
    )

    r_mins, r_secs = divmod(int(remain_time), 60)
    r_hours = 0
    if r_mins >= 60:
        r_hours, r_mins = divmod(r_mins, 60)
    remain_time_str = f"{r_hours:02d}:{r_mins:02d}:{r_secs:02d}"

    size_str = (
        f"{format_size(ctx.progress.download_bytes)}"
        + f"/{format_size(ctx.progress.total_bytes)}"
    )

    if (
        not ctx.rich.tasks
        and ctx.progress.total_files > 0
        and ctx.progress.total_files == ctx.progress.files_completed
    ) or ctx.cancelled:
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="center")
        content = Group("[green]All downloads completed successfully!\n", grid)
        grid.add_row(
            "[white]Total files:",
            f"[green3]{ctx.progress.files_completed}/{ctx.progress.total_files}[/]",
        )
        grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
        grid.add_row("[white]Average Speed:", f"[bold yellow]{speed_str}[/]")
        grid.add_row("[white]Total Time:", f"[bold magenta]{time_str}[/]")
        return Panel(
            grid if ctx.cancelled else content,
            title="[#2e8b57]Final Report",
            border_style="#2e8b57",
            expand=False,
        )

    if ctx.display.dry_run:
        size_str = f"{format_size(ctx.progress.total_bytes)}"
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="center")

        grid.add_row("[white]Total files:", f"[green3]{ctx.progress.total_files}[/]")
        grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
        if ctx.display.verify:
            grid.add_row(
                "[white]Hash Found:",
                f"[bold yellow]{ctx.progress.has_hash}/{ctx.progress.total_files}[/]",
            )
        grid.add_row(
            "[white]Ranges:",
            f"[bold magenta]{ctx.progress.ranges}/{ctx.progress.total_files}[/]",
        )
        return Panel(
            grid,
            title="[#2e8b57]Final Report",
            border_style="#2e8b57",
            expand=False,
        )

    dynamic_title_full = (
        f"\nAvg: [yellow]{speed_str}[/] | "
        f"Remaining Time: [green3]{remain_time_str}[/] | "
        f"Time: [magenta]{time_str}[/] | Download: [bold cyan]{size_str}[/]"
    )

    return Panel(
        ctx.rich.progress,
        title=ctx.rich.dynamic_title + dynamic_title_full,
        border_style="blue",
        padding=(1, 2),
    )


async def print_dry_run_report(
    ctx: UIState, files: dict[int, File], stream: bool, output_dir: str | Path
) -> None:
    """Выводит отчет о том, что БЫЛО БЫ сделано, без фактического скачивания."""

    # 1. Если включен режим JSON логов, отдаем структурированные данные
    if ctx.display.json_logs:
        report_data = {
            "total_files": ctx.progress.total_files,
            "total_bytes": ctx.progress.total_bytes,
            "files": [
                {
                    "filename": f.actual_filename,
                    "size_bytes": f.meta.content_length,
                    "chunks": len(f.chunks),
                    "supports_ranges": f.meta.supports_ranges,
                    "algorithm": f.meta.expected_checksum.algorithm
                    if f.meta.expected_checksum
                    else None,
                    "expected_hash": f.meta.expected_checksum.value
                    if f.meta.expected_checksum
                    else None,
                }
                for f in files.values()
            ],
        }
        # Отправляем в твой универсальный логгер

        await log(
            ctx,
            "DRY_RUN_REPORT",
            status=LogStatus.INFO,
            progress=False,
            throttle_key=None,
            throttle_sec=10,
            **report_data,
        )
        return

    # 2. Если режим тишины (без JSON), просто выходим
    if ctx.display.quiet:
        return

    # 3. Красивый UI для людей
    table = Table(title="[bold yellow] DRY RUN REPORT (No data will be downloaded)[/]")
    table.add_column("Filename", style="cyan", no_wrap=True)
    table.add_column("Size", justify="right")
    table.add_column("Chunks", justify="right")
    if ctx.display.verify:
        table.add_column("Hash Found", justify="center")
    table.add_column("Ranges", justify="center")

    for f in files.values():
        f.create_chunks()
        str_size = format_size(f.meta.content_length)
        if ctx.display.verify and f.meta.expected_checksum:
            ctx.progress.has_hash += 1

        if f.meta.supports_ranges:
            ranges = "✅"
            ctx.progress.ranges += 1
        else:
            ranges = "❌ (Fallback to 1 thread)"

        if ctx.display.verify:
            table.add_row(
                f.meta.original_filename,
                str_size,
                str(len(f.chunks)),
                "✅" if f.meta.expected_checksum else "❌",
                ranges,
            )
        else:
            table.add_row(
                f.meta.original_filename, str_size, str(len(f.chunks)), ranges
            )
    # Печатаем таблицу в stderr (чтобы не сломать пайпы)
    ctx.rich.console.print(table)
    if not stream:
        await check_storage_capacity(ctx, output_path=output_dir)


async def check_storage_capacity(ctx: UIState, output_path: str | Path) -> None:
    """Проверяет наличие свободного места на диске перед началом загрузки."""

    output_dir = Path(output_path).expanduser().resolve()
    check_dir = output_dir if output_dir.exists() else output_dir.parent

    try:
        free_space = shutil.disk_usage(check_dir).free
        required = ctx.progress.total_bytes

        if free_space < required:
            ctx.rich.console.print("\n[bold red] DANGER: Insufficient disk space![/]")
            ctx.rich.console.print(
                f"[red]Required: {format_size(required)} | "
                f"Available: {format_size(free_space)}[/]"
            )
        else:
            ctx.rich.console.print(
                f"\n[bold green] Disk space check passed "
                f"({format_size(free_space)} free).[/]\n"
            )

    except OSError as e:
        if ctx.display.debug:
            raise
        await log(
            ctx,
            f"Warning: Could not check disk space: {e}",
            status=LogStatus.WARNING,
        )


async def handle_exit(ctx: UIState) -> None:
    ctx.is_running = False
    if ctx.rich.live:
        ctx.rich.live.refresh()
        ctx.rich.live.stop()
        if ctx.rich.refresh:
            ctx.rich.refresh.cancel()
    elapsed = time.monotonic() - ctx.progress.start_time
    avg_speed = (
        f"{format_size(ctx.progress.download_bytes / elapsed)}/s" if elapsed > 0 else 0
    )

    size_str = (
        f"{format_size(ctx.progress.download_bytes)}"
        + f"/{format_size(ctx.progress.download_bytes)}"
    )
    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

    status_word = "CANCELLED" if ctx.cancelled else "SUCCESS"
    report = (
        f"\n--- Final Report ({status_word}) ---\n"
        f"Total files:   {ctx.progress.files_completed}/{ctx.progress.total_files}\n"
        f"Total Data:    {size_str}\n"
        f"Average Speed: {avg_speed}\n"
        f"Total Time:    {time_str}\n"
        f"--------------------------------"
    )
    report_dict = {
        "total_files": ctx.progress.files_completed,
        "total_bytes": ctx.progress.download_bytes,
        "average_speed": avg_speed,
        "time_elapsed_sec": elapsed,
    }
    await log(
        ctx,
        report,
        status=LogStatus.INFO,
        progress=False,
        throttle_key=None,
        throttle_sec=10.0,
        **report_dict,
    )


async def ui_start(ctx: UIState) -> None:
    if ctx.is_running:
        return

    ctx.is_running = True
    # 1. ОТКРЫВАЕМ ФАЙЛ ЛОГОВ И ЗАПУСКАЕМ ВОРКЕРА
    if ctx.log.log_file:
        await log_start(ctx)

    if not (ctx.display.no_ui or ctx.display.quiet):
        ctx.rich.progress = Progress(
            SpinnerColumn("aesthetic"),
            TextColumn("[bold yellow]{task.description}"),
            TextColumn(
                "[bold blue]{task.fields[filename]}",
                justify="left",
                table_column=Column(overflow="ellipsis", no_wrap=True, width=30),
            ),
            GradientBar(bar_width=None, finished_style="green"),
            GradientPercent(),
            "•",
            FileSizeColumn(),
            "/",
            TotalFileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=ctx.rich.console,
            transient=False,
            expand=True,
        )

        ctx.rich.live = Live(
            get_renderable=lambda: make_panel(ctx),
            console=ctx.rich.console,
            auto_refresh=True,
            refresh_per_second=10,
            transient=False,
        )
        ctx.rich.live.start()
        ctx.rich.refresh = asyncio.create_task(refresh_loop(ctx))
    ctx.progress.start_time = time.monotonic()
    await log(ctx, "--- Session Started ---")
    await date_print(ctx)


async def ui_stop(ctx: UIState) -> None:
    await handle_exit(ctx)
    await log(ctx, "--- Session Finished ---")

    # 2. КОРРЕКТНО ГАСИМ ЛОГГЕР
    await log_stop(ctx)

    # 3. ЗАКРЫВАЕМ ФАЙЛОВЫЙ ДЕСКРИПТОР
    if ctx.log.log_fd:
        ctx.log.log_fd.close()
        ctx.log.log_fd = None


async def log_start(ctx: UIState) -> None:
    """Запускает фонового воркера (вызывать внутри async_main)"""
    if ctx.log.is_running:
        return

    ctx.log.is_running = True

    try:
        ctx.log.safe_init()
        ctx.log.log_fd = ctx.log.log_file.open("a", encoding="utf-8")
        ctx.log.log_task = asyncio.create_task(log_worker(ctx))
    except OSError as e:
        if ctx.display.debug:
            raise
        ctx.log.log_fd = None

        err = LogFileError(path=str(ctx.log.log_file), original_err=str(e))
        await log(
            ctx,
            f"[bold yellow]LOGGING DISABLED:[/] {err.formatted_msg}",
            status=LogStatus.WARNING,
        )


async def log_stop(ctx: UIState) -> None:
    if ctx.log.log_task:
        ctx.log.log_queue.put_nowait(None)
        await ctx.log.log_task
