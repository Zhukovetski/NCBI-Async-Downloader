from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from hydrastream.facade import HydraClient
from hydrastream.main import app, async_main

# --- ТЕСТЫ ДЛЯ facade.py (Строки 23, 34, 42, 47-48, 53-54) ---


@pytest.mark.asyncio
async def test_facade_run_mode(mocker: MockerFixture) -> None:
    mock_run: AsyncMock = mocker.patch(
        "hydrastream.facade.run_downloads", new_callable=AsyncMock
    )
    """Покрытие строк 23-31, 33-42, 44-48"""

    # 1. Проверяем инициализацию и контекстный менеджер (строки 23, 34, 42)
    async with HydraClient(threads=5, out_dir="test_dir", quiet=True) as client:
        assert client.config.threads == 5
        assert client.config.out_dir == "test_dir"
        assert client.config.quiet is True

        # 2. Проверяем вызов run() (строки 47-48)
        await client.run(["http://test.com"], {"http://test.com": "abc"})

        # Проверяем, что HydraContext был создан и передан в engine.run_downloads
        mock_run.assert_called_once()

        # Достаем аргументы, с которыми вызвали run_downloads
        args, _ = mock_run.call_args
        ctx_passed = args[0]
        links_passed = args[1]

        assert ctx_passed.config == client.config
        assert links_passed == ["http://test.com"]


@pytest.mark.asyncio
async def test_facade_stream_mode(mocker: MockerFixture) -> None:
    mock_stream_all: MagicMock = mocker.patch("hydrastream.facade.stream_all")
    """Покрытие строк 50-54"""

    # mock_stream_all должен вернуть генератор (имитируем это списком)
    async def mock_generator() -> AsyncGenerator[tuple[str, AsyncMock]]:
        yield ("file.txt", AsyncMock())

    mock_stream_all.return_value = mock_generator()

    client = HydraClient(threads=2)

    # Проверяем вызов stream()
    generator = client.stream(["http://test.com"])

    # Итерируемся, чтобы спровоцировать вызов (так как это генератор)
    [item async for item in generator]
    mock_stream_all.assert_called_once()
    args, _ = mock_stream_all.call_args
    assert args[0].config == client.config


# --- ТЕСТЫ ДЛЯ main.py (CLI и async_main) ---

# Инициализируем виртуального бегуна для Typer
runner = CliRunner()


@pytest.mark.asyncio
async def test_async_main_run_mode(mocker: MockerFixture) -> None:
    mock_client_class: MagicMock = mocker.patch("hydrastream.main.HydraClient")
    """Покрытие async_main (режим записи на диск)"""
    # Настраиваем мок клиента
    mock_client_instance = AsyncMock()
    # async with HydraClient() возвращает инстанс:
    mock_client_class.return_value.__aenter__.return_value = mock_client_instance

    await async_main(
        links=["http://a.com"],
        stream=False,  # <-- Режим Диска
        threads=2,
        no_ui=True,
        quiet=False,
        output_dir="out",
        md5="123",
        chunk_timeout=10.0,
        stream_buffer_size=100,
    )

    # Проверяем, что вызвался метод run(), а не stream()
    mock_client_instance.run.assert_called_once_with(
        ["http://a.com"], {"http://a.com": "123"}
    )


@pytest.mark.asyncio
async def test_async_main_stream_mode(
    mocker: MockerFixture,
) -> None:
    mock_sys: MagicMock = mocker.patch("hydrastream.main.sys")
    mock_client_class: MagicMock = mocker.patch("hydrastream.main.HydraClient")
    mock_sys.__stdout__ = MagicMock()
    """Покрытие async_main (режим стриминга и защиты терминала)"""

    # 1. Настраиваем фейковый генератор байтов
    async def fake_byte_gen() -> AsyncGenerator[bytes]:
        yield b"chunk1"
        yield b"chunk2"

    # 2. Добавь аргументы, так как stream их принимает
    async def fake_file_gen(
        *args: object, **kwargs: object
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
        yield "test.bin", fake_byte_gen()

    mock_client_instance = AsyncMock()
    mock_client_instance.stream = MagicMock(return_value=fake_file_gen())
    mock_client_class.return_value.__aenter__.return_value = mock_client_instance

    # 2. Имитируем, что вывод перенаправлен в трубу (isatty = False)
    mock_sys.__stdout__.isatty.return_value = False
    mock_sys.stdout.buffer = MagicMock()

    await async_main(
        links=["http://a.com"],
        stream=True,  # <-- Режим Стрима
        threads=2,
        no_ui=True,
        quiet=True,
        output_dir="out",
        md5=None,
        chunk_timeout=10.0,
        stream_buffer_size=100,
    )

    # Проверяем, что байты улетели в sys.stdout.buffer
    assert mock_sys.stdout.buffer.write.call_count == 2
    mock_sys.stdout.buffer.write.assert_any_call(b"chunk1")
    mock_sys.stdout.buffer.flush.assert_called()


def test_cli_no_args() -> None:
    """Проверка, что Typer падает с ошибкой, если нет ссылок"""
    result = runner.invoke(app, [])
    assert result.exit_code == 2  # Typer возвращает 2 при ошибке парсинга аргументов
    assert "Usage: cli [OPTIONS] LINKS..." in result.stderr


def test_cli_valid_args(mocker: MockerFixture) -> None:
    mock_asyncio_run: MagicMock = mocker.patch("hydrastream.main.asyncio.run")
    """Проверка, что CLI правильно парсит аргументы и передает их в asyncio"""
    result = runner.invoke(
        app, ["http://test.com", "-t", "5", "--quiet", "--md5", "abc"]
    )

    assert result.exit_code == 0
    mock_asyncio_run.assert_called_once()

    # Достаем корутину, которую передали в asyncio.run()
    args, _ = mock_asyncio_run.call_args
    passed_coro = args[0]
    assert passed_coro.__name__ == "async_main"
    coro_to_cleanup = mock_asyncio_run.call_args[0][0]

    # 2. ПРОВЕРКА (твой старый ассерт)
    assert coro_to_cleanup.__name__ == "async_main"

    # 3. ЗАКРЫВАЕМ ЕЁ (Это уберет ворнинг!)
    coro_to_cleanup.close()


def test_cli_keyboard_interrupt(mocker: MockerFixture) -> None:
    mock_asyncio_run: MagicMock = mocker.patch(
        "hydrastream.main.asyncio.run", side_effect=KeyboardInterrupt()
    )
    """Проверка перехвата Ctrl+C в CLI"""
    result = runner.invoke(app, ["http://test.com"])

    assert result.exit_code == 130  # Твой кастомный код возврата для SIGINT!
    assert "Interrupted by user" in result.stderr
    coro_to_cleanup = mock_asyncio_run.call_args[0][0]

    # 2. ПРОВЕРКА (твой старый ассерт)
    assert coro_to_cleanup.__name__ == "async_main"

    # 3. ЗАКРЫВАЕМ ЕЁ (Это уберет ворнинг!)
    coro_to_cleanup.close()


def test_cli_critical_error(mocker: MockerFixture) -> None:
    mock_asyncio_run: MagicMock = mocker.patch(
        "hydrastream.main.asyncio.run", side_effect=ValueError("Test Explosion")
    )
    """Проверка перехвата критических ошибок в CLI"""
    result = runner.invoke(app, ["http://test.com"])

    assert result.exit_code == 1
    assert "Critical error" in result.stderr
    assert "Test Explosion" in result.stderr
    coro_to_cleanup = mock_asyncio_run.call_args[0][0]

    # 2. ПРОВЕРКА (твой старый ассерт)
    assert coro_to_cleanup.__name__ == "async_main"

    # 3. ЗАКРЫВАЕМ ЕЁ (Это уберет ворнинг!)
    coro_to_cleanup.close()
