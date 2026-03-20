import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from hydrastream.engine import (
    _stream_one,
    run_downloads,
    stream_all,
)
from hydrastream.models import HydraContext


# --- ФИКСТУРЫ ---
@pytest.fixture
def mock_ctx() -> MagicMock:
    """Создаем идеальный фейковый контекст для тестов"""
    ctx = MagicMock(spec=HydraContext)
    ctx.is_running = True
    ctx.stream = False
    ctx.config.threads = 2

    # Мокаем подсистемы
    ctx.ui = AsyncMock()
    ctx.net = AsyncMock()
    ctx.fs = AsyncMock()

    ctx.ui.live.stop = MagicMock(return_value=None)  # Явно синхронный!
    ctx.ui.refresh.cancel = MagicMock(return_value=None)

    # Очереди оставляем настоящими, чтобы проверять логику!
    ctx.files = {}
    ctx.heap = []
    ctx.chunk_queue = AsyncMock()
    ctx.stream_queue = asyncio.Queue()
    ctx.file_discovery_queue = asyncio.Queue()
    ctx.condition = asyncio.Condition()

    # Для успешного завершения (строки 218-227)
    ctx.ui.total_files = 1
    ctx.ui.files_completed = 1
    ctx.ui.total_bytes = 1
    ctx.ui.download_bytes = 1

    return ctx


# --- ТЕСТ 1: Исключения и отмена в run_downloads (Строки 198, 229-234, 250) ---
@pytest.mark.asyncio
async def test_run_downloads_exceptions(
    mocker: MockerFixture,
    mock_ctx: MagicMock,
) -> None:
    mock_log: AsyncMock = mocker.patch("hydrastream.engine.log", new_callable=AsyncMock)
    mocker.patch("hydrastream.engine.chunk_producer", new_callable=AsyncMock)
    mocker.patch("hydrastream.engine.run_dispatch_loop", new_callable=AsyncMock)
    mock_ctx.live = MagicMock()
    # 1. Тестируем строку 198: передача строки вместо списка
    mock_ctx.condition.wait_for = AsyncMock(side_effect=asyncio.CancelledError())
    await run_downloads(mock_ctx, "http://single-link.com")
    assert mock_ctx.stream is False

    # 2. Тестируем строки 232-234: Критическая ошибка
    mock_ctx.condition.wait_for.side_effect = Exception("Boom!")
    with pytest.raises(Exception, match="Boom!"):
        await run_downloads(mock_ctx, ["http://link.com"])

    # Проверяем, что ошибка залогировалась
    mock_log.assert_awaited_with(
        mock_ctx.ui,  # Первый аргумент функции log
        "Runtime Exception in run(): Boom!",  # Сообщение
        status="CRITICAL",  # Именованный аргумент
    )

    # 3. Тестируем закрытие fd (строка 250)
    mock_file = MagicMock()
    mock_ctx.files = {"test": mock_file}
    # Вызываем аварийный выход (чтобы сработал finally)
    mock_ctx.condition.wait_for.side_effect = asyncio.CancelledError()
    await run_downloads(mock_ctx, ["http://link.com"])
    mock_file.close_fd.assert_called()


# --- ТЕСТ 2: Магия кучи в _stream_one (Строки 141-154, 160-183) ---
@pytest.mark.asyncio
async def teststream_one_reordering(
    mocker: MockerFixture,
    mock_ctx: MagicMock,
) -> None:
    mock_done: AsyncMock = mocker.patch(
        "hydrastream.engine.done", new_callable=AsyncMock
    )
    mocker.patch("hydrastream.engine.verify_stream")
    """Самый крутой тест: проверяем выпрямление потока через heapq"""

    # Подготавливаем фейковый файл размером 10 байт
    mock_file = MagicMock()
    mock_file.meta.content_length = 10
    mock_file.meta.expected_md5 = None
    mock_ctx.files["test.txt"] = mock_file

    # ИМИТИРУЕМ ХАОС СЕТИ: Кладем куски ВРАЗНОБОЙ
    # Сначала кладем второй кусок (смещение 5)
    await mock_ctx.stream_queue.put((5, bytearray(b"world")))
    # Потом кладем первый кусок (смещение 0)
    await mock_ctx.stream_queue.put((0, bytearray(b"hello")))

    # Запускаем генератор
    gen = _stream_one(mock_ctx, "test.txt")

    # Итерация 1: Должен выдать "hello" (из очереди), а "world" спрятать в кучу
    res1 = await anext(gen)
    assert res1 == b"hello"
    assert len(mock_ctx.heap) == 1  # "world" лежит в куче!

    # Итерация 2: Должен достать "world" из кучи (строки 141-154)
    res2 = await anext(gen)
    assert res2 == b"world"
    assert len(mock_ctx.heap) == 0

    # Генератор должен корректно закрыться (строки 172-183)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)

    mock_done.assert_called_once_with(mock_ctx.ui, "test.txt")


# --- ТЕСТ 3: Отмена и пропуски в stream_all (Строки 58, 73-79) ---
@pytest.mark.asyncio
async def test_stream_all_flow(
    mocker: MockerFixture,
    mock_ctx: MagicMock,
) -> None:
    mocker.patch("hydrastream.engine.run_dispatch_loop", new_callable=AsyncMock)
    mocker.patch("hydrastream.engine.chunk_producer", new_callable=AsyncMock)
    mock_ctx.live = MagicMock()
    # Строка 58: передаем строку
    links = "http://test.com"

    # Имитируем, что Продюсер не смог скачать файл (вернул None)
    await mock_ctx.file_discovery_queue.put(None)

    # Запускаем генератор
    gen = stream_all(mock_ctx, links)

    # Так как прилетел None, генератор должен пропустить файл (continue на строке 79)
    # и завершиться (StopAsyncIteration)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)
