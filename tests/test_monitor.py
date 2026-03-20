import asyncio
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_mock import MockerFixture
from rich.panel import Panel
from rich.progress import (
    Task,
    TaskID,
)

from hydrastream.models import UIState
from hydrastream.monitor import (
    GradientBar,
    GradientPercent,
    add_file,
    date_print,
    done,
    get_gradient_color,
    handle_exit,
    log,
    make_panel,
    refresh_loop,
    truncate_filename,
    ui_start,
    update,
    write_log,
)

# --- ФИКСТУРЫ ---


@pytest.fixture
def mock_ui_state(tmp_path: Path) -> UIState:
    """Создаем чистый стейт для тестов, пишущий логи во временную папку"""
    log_file = tmp_path / "test.log"

    return UIState(
        no_ui=False,
        quiet=False,
        log_file=log_file,
        console=MagicMock(),  # Мокаем консоль, чтобы она не сыпала на экран
    )


# --- ТЕСТЫ БАЗОВЫХ УТИЛИТ (Строки 36, 40-45) ---


def test_truncate_filename() -> None:
    assert truncate_filename("short.txt", w=20) == "short.txt"
    assert (
        truncate_filename("this_is_a_very_long_filename.txt", w=20)
        == "this_is_a...name.txt"
    )


def test_get_gradient_color() -> None:
    # Проверяем граничные значения математики цвета
    assert get_gradient_color(-10) == "#ff0000"  # Красный
    assert get_gradient_color(0) == "#ff0000"  # Красный
    assert (
        get_gradient_color(50) == "#ffff00"
    )  # Должен быть желтый (у тебя математика дает 255, 255, 0 при >=50)
    assert get_gradient_color(100) == "#00ff00"  # Зеленый
    assert get_gradient_color(150) == "#00ff00"  # Зеленый


# --- ТЕСТЫ RICH COLUMNS (Строки 50-56, 61-65) ---


def test_gradient_columns(mock_ui_state: UIState) -> None:
    """Тестируем рендеринг кастомных колонок"""

    mock_task = MagicMock(spec=Task)
    mock_task.total = None
    mock_task.completed = 0
    mock_task.finished = False
    mock_task.percentage = 50.0

    bar = GradientBar()
    # Тест: total=None (строка 50)
    bar.render(mock_task)
    assert bar.complete_style == "cyan"

    # Тест: finished=True (строки 52-53)
    mock_task.total = 100
    mock_task.finished = True
    bar.render(mock_task)
    assert bar.complete_style == "bold bright_green blink"

    percent = GradientPercent()
    # Тест: total=None для процентов (строки 61-62)
    mock_task.total = None
    assert percent.render(mock_task).plain == " CALC "


# --- ТЕСТЫ ЛОГИРОВАНИЯ (Строки 70, 76-77, 85, 92-115, 122-149) ---


def test_write_log(mock_ui_state: UIState) -> None:
    """Строки 70, 76-77: Запись в файл и обработка OSError"""
    write_log(mock_ui_state, "Test Message")
    assert "Test Message" in mock_ui_state.log_file.read_text()

    # Имитация ошибки доступа (OSError) - строка 77
    with patch("pathlib.Path.open", side_effect=OSError):
        write_log(mock_ui_state, "Error Message")  # Не должен упасть!


@pytest.mark.asyncio
async def test_date_print(mock_ui_state: UIState) -> None:
    """Строки 85, 86: Печать даты"""
    await date_print(mock_ui_state)
    cast(MagicMock, mock_ui_state.console.print).assert_called()
    assert "---" in mock_ui_state.log_file.read_text()


@pytest.mark.asyncio
async def test_log_formatting_and_throttling(mock_ui_state: UIState) -> None:
    """Строки 92-115 (formatting), 122-149 (log logic)"""
    mock_ui_state.progress = MagicMock()  # Имитируем включенный UI

    # 1. Обычный лог
    await log(mock_ui_state, "Hello", status="INFO")
    # INFO не принтится в консоль при прогрессе
    mock_ui_state.progress.console.print.assert_not_called()

    # 2. Ошибка (должен быть вызван print)
    await log(mock_ui_state, "Boom", status="ERROR")
    mock_ui_state.progress.console.print.assert_called()

    # 3. Троттлинг (строки 128-132)
    await log(mock_ui_state, "Spam", throttle_key="test", throttle_sec=10)
    # Пытаемся залогировать еще раз мгновенно - должно быть проигнорировано
    await log(mock_ui_state, "Spam2", throttle_key="test", throttle_sec=10)

    logs = mock_ui_state.log_file.read_text()
    assert "Spam" in logs
    assert "Spam2" not in logs

    # 4. Quiet режим (строки 141-142)
    mock_ui_state.quiet = True
    await log(mock_ui_state, "Quiet", status="ERROR")
    # Консоль не должна была вызываться в quiet режиме
    cast(MagicMock, mock_ui_state.console.print).assert_not_called()


# --- ТЕСТЫ УПРАВЛЕНИЯ ЗАДАЧАМИ (Строки 158-168, 179-196, 212-226) ---


def test_add_and_update_file(mock_ui_state: UIState) -> None:
    """Строки 158-168, 173-174: add_file и update"""
    mock_ui_state.progress = MagicMock()
    mock_ui_state.progress.add_task.return_value = 1

    # add_file
    add_file(mock_ui_state, "test.bin", 100)
    assert mock_ui_state.total_bytes == 100
    assert mock_ui_state.total_files == 1
    assert mock_ui_state.tasks["test.bin"] == 1

    # update
    update(mock_ui_state, "test.bin", 50)
    assert mock_ui_state.buffer["test.bin"] == 50
    assert mock_ui_state.download_bytes == 50


@pytest.mark.asyncio
async def test_refresh_loop(mock_ui_state: UIState) -> None:
    """Строки 179-196: фоновый цикл UI"""
    mock_ui_state.progress = MagicMock()
    mock_ui_state.is_running = True
    mock_ui_state.tasks["test.bin"] = TaskID(1)
    mock_ui_state.buffer["test.bin"] = 50
    mock_ui_state.renewal_rate = 0.01

    # Запускаем цикл как фоновую задачу
    task = asyncio.create_task(refresh_loop(mock_ui_state))
    await asyncio.sleep(0.05)  # Даем ему сделать 1-2 круга

    mock_ui_state.is_running = False  # Выключаем цикл
    await task

    # Проверяем, что буфер сбросился и active_files обновились
    assert mock_ui_state.buffer["test.bin"] == 0
    assert "test.bin" in mock_ui_state.active_files
    mock_ui_state.progress.update.assert_called()


@pytest.mark.asyncio
async def test_done(mocker: MockerFixture, mock_ui_state: UIState) -> None:
    mock_log: MagicMock = mocker.patch(
        "hydrastream.monitor.log", new_callable=AsyncMock
    )
    """Строки 212-226: Завершение файла"""
    mock_ui_state.progress = MagicMock()
    mock_ui_state.tasks["test.bin"] = TaskID(1)
    mock_ui_state.active_files.add("test.bin")

    # Мокаем внутреннюю структуру rich
    mock_task = MagicMock()
    mock_task.total = 100
    mock_ui_state.progress.tasks = {1: mock_task}

    await done(mock_ui_state, "test.bin")

    assert "test.bin" not in mock_ui_state.tasks
    assert "test.bin" not in mock_ui_state.active_files
    assert mock_ui_state.files_completed == 1
    mock_log.assert_called_with(
        mock_ui_state, "Done: test.bin", status="SUCCESS", progress=True
    )


# --- ТЕСТЫ РЕНДЕРИНГА И СТАРТ/СТОП (Строки 231-294, 304-306, 340-370) ---


def test_make_panel_empty(mocker: MockerFixture, mock_ui_state: UIState) -> None:
    mocker.patch("time.monotonic", return_value=100.0)
    """Строки 231-236: пустая панель"""
    assert make_panel(mock_ui_state) == ""

    mock_ui_state.progress = MagicMock()
    mock_ui_state.progress.tasks = []
    assert make_panel(mock_ui_state) == ""


def test_make_panel_active(mocker: MockerFixture, mock_ui_state: UIState) -> None:
    mocker.patch("time.monotonic", return_value=60.0)
    """Строки 237-266, 288-294: активная панель"""
    mock_ui_state.progress = MagicMock()
    mock_ui_state.progress.tasks = [1]
    mock_ui_state.tasks = {"test": TaskID(1)}  # Есть активные задачи

    mock_ui_state.start_time = 0.0
    mock_ui_state.download_bytes = 1024 * 1024 * 60  # 60 MB
    mock_ui_state.total_bytes = 1024 * 1024 * 120  # 120 MB

    panel = make_panel(mock_ui_state)
    assert isinstance(panel, Panel)
    # Проверяем, что скорость посчиталась как 1 MB/s
    assert "1.00 MB/s" in str(panel.title)


@pytest.mark.asyncio
async def test_ui_start_stop(mocker: MockerFixture, mock_ui_state: UIState) -> None:
    mocker.patch("hydrastream.monitor.date_print", new_callable=AsyncMock)
    mock_live_class: MagicMock = mocker.patch("hydrastream.monitor.Live")

    # Настраиваем, чтобы при создании Live() возвращался мок
    mock_live_instance = mock_live_class.return_value

    mock_ui_state.no_ui = False
    mock_ui_state.quiet = False

    # Запускаем
    await ui_start(mock_ui_state)

    # Теперь проверяем через инстанс, который вернул патч
    mock_live_instance.start.assert_called_once()

    # Останавливаем
    await handle_exit(mock_ui_state, cancelled=True)

    mock_live_instance.stop.assert_called_once()
