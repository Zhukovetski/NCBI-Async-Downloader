import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pytest_mock import MockerFixture

from hydrastream.dispatcher import (
    disk_process_chunk,
    file_done,
    get_chunk,
    process_chunk,
    run_dispatch_loop,
    stream_process_chunk,
)
from hydrastream.models import Chunk


# --- ФИКСТУРЫ ---
@pytest.fixture
def mock_ctx() -> MagicMock:
    """Создает фейковый HydraContext для тестов"""
    ctx = MagicMock()
    ctx.stream = False
    ctx.is_running = True
    ctx.current_file = "test.bin"
    ctx.heap = []
    ctx.heap_size = 10
    ctx.config.chunk_timeout = 60

    # Мокаем очереди
    ctx.chunk_queue = AsyncMock()
    ctx.stream_queue = AsyncMock()
    ctx.condition = AsyncMock()

    # Мокаем словарь файлов
    mock_file = MagicMock()
    mock_file.is_failed = False
    mock_file.fd = 999
    mock_file.meta.url = "http://test"
    ctx.files = {"test.bin": mock_file}

    return ctx


@pytest.fixture
def mock_chunk() -> Chunk:
    return Chunk(filename="test.bin", start=0, end=100, current_pos=0)


# --- ТЕСТЫ ДЛЯ file_done (Строки 20-35) ---


@pytest.mark.asyncio
async def test_file_done_stream(mock_ctx: MagicMock, mock_chunk: Chunk) -> None:
    """Покрытие строки 20-21: быстрый выход если stream = True"""
    mock_ctx.stream = True
    await file_done(mock_ctx, mock_chunk)
    # Проверяем, что файлы не удалялись
    assert "test.bin" in mock_ctx.files


@pytest.mark.asyncio
async def test_file_done_disk_success(
    mocker: MockerFixture,
    mock_ctx: MagicMock,
    mock_chunk: Chunk,
) -> None:
    mock_done: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.done", new_callable=AsyncMock
    )
    mock_del_state: MagicMock = mocker.patch("hydrastream.dispatcher.delete_state")
    mock_v_size: MagicMock = mocker.patch("hydrastream.dispatcher.verify_size")
    mock_v_hash: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.verify_file_hash", new_callable=AsyncMock
    )
    """Покрытие строк 23-35: успешное завершение файла на диске"""
    mock_ctx.condition.notify_all = MagicMock()
    mock_file = mock_ctx.files["test.bin"]

    mock_v_hash.return_value = True  # Хеш совпал

    await file_done(mock_ctx, mock_chunk)

    # Проверяем, что все нужные функции были вызваны
    mock_v_hash.assert_called_once()
    mock_file.close_fd.assert_called_once()
    mock_v_size.assert_called_once()
    mock_del_state.assert_called_once()
    mock_done.assert_called_once()

    # Проверяем, что файл удален из словаря и вызван notify_all
    assert "test.bin" not in mock_ctx.files
    mock_ctx.condition.notify_all.assert_called_once()


# --- ТЕСТЫ ДЛЯ get_chunk (Строки 42, 45-46) ---


@pytest.mark.asyncio
async def test_get_chunk_file_failed(mock_ctx: MagicMock, mock_chunk: Chunk) -> None:
    """Покрытие строки 42: возврат None, если файл failed"""
    mock_ctx.chunk_queue.get.return_value = (10, mock_chunk)
    mock_ctx.files["test.bin"].is_failed = True

    result = await get_chunk(mock_ctx)
    assert result is None


@pytest.mark.asyncio
async def test_get_chunk_stream_backpressure(
    mock_ctx: MagicMock, mock_chunk: Chunk
) -> None:
    """Покрытие строк 45-46: ожидание нужного файла в стриме"""
    mock_ctx.stream = True
    mock_ctx.current_file = "other.bin"  # Текущий файл другой!
    mock_ctx.chunk_queue.get.return_value = (10, mock_chunk)

    await get_chunk(mock_ctx)
    # Проверяем, что воркер попытался уснуть (вызвал wait_for)
    mock_ctx.condition.wait_for.assert_called_once()


# --- ТЕСТЫ ДЛЯ run_dispatch_loop (Строки 59, 61, 67-82) ---


@pytest.mark.asyncio
async def test_dispatch_loop_404_error(
    mocker: MockerFixture,
    mock_ctx: MagicMock,
    mock_chunk: Chunk,
) -> None:
    mock_log: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.log", new_callable=AsyncMock
    )
    mock_process: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.process_chunk", new_callable=AsyncMock
    )
    mock_get: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.get_chunk", new_callable=AsyncMock
    )
    """Покрытие строк 67-74: Фатальная ошибка 404"""
    mock_ctx.chunk_queue.task_done = MagicMock()
    # 1 раз возвращаем чанк, 2 раз кидаем CancelledError чтобы выйти из цикла
    mock_get.side_effect = [mock_chunk, asyncio.CancelledError()]

    # Имитируем 404 ошибку
    resp = MagicMock()
    resp.status_code = 404
    mock_process.side_effect = httpx.HTTPStatusError(
        "404", request=MagicMock(), response=resp
    )

    await run_dispatch_loop(mock_ctx)

    # Проверяем, что файл помечен как failed и залогирована ошибка
    mock_log.assert_called_once()
    assert mock_ctx.files["test.bin"].is_failed is True


@pytest.mark.asyncio
async def test_dispatch_loop_timeout_error(
    mocker: MockerFixture,
    mock_ctx: MagicMock,
    mock_chunk: Chunk,
) -> None:
    mock_sleep: AsyncMock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    mock_process: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.process_chunk", new_callable=AsyncMock
    )
    mock_get: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.get_chunk", new_callable=AsyncMock
    )
    """Покрытие строк 80-82: Сетевая ошибка (Timeout)"""
    mock_ctx.chunk_queue.task_done = MagicMock()
    mock_get.side_effect = [mock_chunk, asyncio.CancelledError()]
    mock_process.side_effect = TimeoutError("Network drop")

    await run_dispatch_loop(mock_ctx)

    # Проверяем, что чанк вернулся в очередь с приоритетом -1
    mock_ctx.chunk_queue.put.assert_called_with((-1, mock_chunk))
    mock_sleep.assert_called_once()


# --- ТЕСТЫ ДЛЯ process_chunk и process_disk (Строки 94-117, 134-146, 152) ---


@pytest.mark.asyncio
async def test_process_chunk_already_finished(mock_ctx: MagicMock) -> None:
    """Покрытие строки 152: выход, если чанк уже скачан"""
    chunk = Chunk(filename="test.bin", start=0, end=100, current_pos=101)
    await process_chunk(mock_ctx, chunk)
    # Если мы тут, значит функция сделала return и ничего не сломалось


@contextlib.asynccontextmanager
async def mock_stream_cm(*args: object, **kwargs: object) -> AsyncIterator[MagicMock]:
    """Фейковый стрим, который возвращает байты"""
    mock_resp = MagicMock()

    async def fake_aiter() -> AsyncGenerator[bytes]:
        yield b"hello "
        yield b"world"

    mock_resp.aiter_bytes = fake_aiter
    yield mock_resp


@pytest.mark.asyncio
async def test_disk_process_chunk(
    mocker: MockerFixture, mock_ctx: MagicMock, mock_chunk: Chunk
) -> None:
    mock_update: MagicMock = mocker.patch("hydrastream.dispatcher.update")
    mock_write: AsyncMock = mocker.patch(
        "hydrastream.dispatcher.write_chunk_data", new_callable=AsyncMock
    )
    mocker.patch("hydrastream.dispatcher.stream_chunk", side_effect=mock_stream_cm)
    """Покрытие строк 94-117: успешная запись на диск"""
    headers = {"Range": "bytes=0-100"}

    await disk_process_chunk(mock_ctx, mock_chunk, headers)

    # Проверяем, что данные были записаны в буфер и сброшены на диск
    assert mock_write.call_count >= 1
    assert mock_update.call_count == 2  # 2 куска из aiter_bytes
    # current_pos должен был сдвинуться на длину "hello world" (11 байт)
    assert mock_chunk.current_pos == 11


@pytest.mark.asyncio
async def test_stream_process_chunk(
    mocker: MockerFixture, mock_ctx: MagicMock, mock_chunk: Chunk
) -> None:
    mocker.patch("hydrastream.dispatcher.update")
    mocker.patch("hydrastream.dispatcher.stream_chunk", side_effect=mock_stream_cm)
    """Покрытие строк 134-139: успешный стриминг в очередь"""
    headers = {"Range": "bytes=0-100"}
    mock_ctx.heap = [1] * 20  # Имитируем полную кучу
    mock_ctx.heap_size = 10

    await stream_process_chunk(mock_ctx, mock_chunk, headers)

    # Проверяем бэкпрешер (куча была полна, значит воркер должен был ждать)
    mock_ctx.condition.wait_for.assert_called()

    # Проверяем, что байты упали в очередь стрима
    mock_ctx.stream_queue.put.assert_called_once_with((0, bytearray(b"hello world")))
