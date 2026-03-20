import email.utils
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, AsyncMock, MagicMock

import httpx
import pytest
from pytest_mock import MockerFixture

from hydrastream.models import NetworkState
from hydrastream.network import (
    _evaluate_failure,
    _get_retry_after,
    acquire,
    extract_filename,
    report_429,
    stream_chunk,
    try_scale_up,
)


# --- ФИКСТУРЫ ---
@pytest.fixture
def mock_net_ctx() -> NetworkState:
    """Создает фейковый NetworkState с замоканным монитором"""
    monitor_mock = MagicMock()
    monitor_mock.log = AsyncMock()  # Обязательно AsyncMock, т.к. await log(...)
    monitor_mock.log_throttle = {}
    # Инициализируем через __post_init__
    return NetworkState(threads=2, monitor=monitor_mock)


# ==========================================
# 1. ТЕСТЫ ПАРСИНГА (Строки 244-288)
# ==========================================


def test_extract_filename() -> None:
    """Покрытие строк 265, 270, 278, 283-288"""

    # 1. RFC 5987 (UTF-8)
    h1 = httpx.Headers({
        "Content-Disposition": "attachment; "
        "filename*=UTF-8''%D1%84%D0%B0%D0%B9%D0%BB.txt"
    })
    assert extract_filename("http://url", h1) == "файл.txt"

    # 2. Стандартный Content-Disposition
    h2 = httpx.Headers({"Content-Disposition": 'attachment; filename="data.bin"'})
    assert extract_filename("http://url", h2) == "data.bin"

    # 3. Mimetype guessing (по Content-Type)
    h3 = httpx.Headers({"Content-Type": "application/pdf"})
    assert extract_filename("http://url/", h3) == "downloaded_file.pdf"

    # 4. Очистка URL от мусора
    h4 = httpx.Headers()
    assert extract_filename("http://url/file.txt?token=123#anchor", h4) == "file.txt"


def test_get_retry_after() -> None:
    """Покрытие строк 244-255"""
    # 1. Формат секунд
    resp1 = httpx.Response(429, headers={"Retry-After": "15"})
    assert _get_retry_after(resp1) == 15.0

    # 2. Дата в будущем (RFC 2822)

    future = datetime.now(UTC) + timedelta(seconds=30)
    date_str = email.utils.format_datetime(future)

    resp2 = httpx.Response(429, headers={"Retry-After": date_str})
    val = _get_retry_after(resp2)
    assert val is not None
    assert 25 < val <= 30  # Должно быть около 30 секунд

    # 3. Мусор в заголовке
    resp3 = httpx.Response(429, headers={"Retry-After": "garbage"})
    assert _get_retry_after(resp3) is None


# ==========================================
# 2. ТЕСТЫ AIMD ЛИМИТЕРА (Строки 50-110)
# ==========================================


@pytest.mark.asyncio
async def test_try_scale_up(mock_net_ctx: NetworkState) -> None:
    """Покрытие строк 83, 88-90"""
    amid = mock_net_ctx.rate_limiter
    amid.current_rps = 3
    amid.max_rps = 4
    amid.last_429_time = 0.0  # Сбрасываем кулдаун

    # Успешный скейл-ап
    assert await try_scale_up(amid) is True
    assert amid.current_rps == 4

    # Уперлись в потолок (max_rps)
    assert await try_scale_up(amid) is False


@pytest.mark.asyncio
async def test_report_429_and_acquire(
    mocker: MockerFixture, mock_net_ctx: NetworkState
) -> None:
    mock_log: AsyncMock = mocker.patch(
        "hydrastream.network.log", new_callable=AsyncMock
    )
    """Покрытие строк 50-72, 100-102"""
    amid = mock_net_ctx.rate_limiter
    amid.current_rps = 4
    amid.min_rps = 1

    # 1. Обычный 429 (Срезаем скорость в 2 раза)
    await report_429(amid, retry_after=None)
    assert amid.current_rps == 2
    mock_log.assert_awaited_with(
        mock_net_ctx.monitor,  # Первый аргумент функции - это monitor
        ANY,  # Текст сообщения (можно ANY из unittest.mock)
        status="WARNING",  # Или какой там статус у 429 ошибки
    )

    # 2. Жесткий 429 (Circuit Breaker)
    await report_429(amid, retry_after=10.0)
    assert amid.circuit_broken_until > time.time()
    assert amid.current_rps == 2  # Скорость уже не падает, мы в локдауне

    # 3. Тестируем acquire (ожидание Circuit Breaker)
    mock_sleep: AsyncMock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    # Настраиваем логику
    def side_effect_reset(_: object) -> None:
        amid.circuit_broken_until = 0

    mock_sleep.side_effect = side_effect_reset
    # ... ваш код ...
    async with acquire(amid):
        pass

    mock_sleep.assert_called_once()


# ==========================================
# 3. ТЕСТЫ ОШИБОК И СТРИМА (Строки 130-163, 213-240)
# ==========================================


@pytest.mark.asyncio
async def test_evaluate_failure_http(
    mocker: MockerFixture, mock_net_ctx: NetworkState
) -> None:
    mock_log: AsyncMock = mocker.patch(
        "hydrastream.network.log", new_callable=AsyncMock
    )
    mocker.patch("hydrastream.network._get_retry_after", return_value=5.0)
    """Покрытие строк 130-144"""
    # Серверу плохо (502)
    resp = httpx.Response(502, request=MagicMock())
    delay = await _evaluate_failure(mock_net_ctx, "http://test", 1, resp, None)
    assert delay == 5.0  # Взял из заголовка

    # Нас забанили (429) -> должен сработать report_429
    resp429 = httpx.Response(429, request=MagicMock())
    await _evaluate_failure(mock_net_ctx, "http://test", 1, resp429, None)
    assert mock_log.await_args is not None
    args, kwargs = mock_log.await_args
    assert "Attempt 1 failed (429)" in args[1]
    assert kwargs["status"] == "WARNING"
    assert kwargs["throttle_key"] == "net_slow"


@pytest.mark.asyncio
async def test_evaluate_failure_exception(mock_net_ctx: NetworkState) -> None:
    """Покрытие строк 158-163"""
    # Необратимая ошибка (ValueError или SyntaxError в коде)
    exc = ValueError("Boom")
    delay = await _evaluate_failure(mock_net_ctx, "http://test", 1, None, exc)
    assert delay is None  # Фатально!


@pytest.mark.asyncio
async def test_stream_chunk_exceptions(
    mocker: MockerFixture, mock_net_ctx: NetworkState
) -> None:
    # Мокаем сон, чтобы тест прошел за 0.01с
    mock_sleep: AsyncMock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    """Покрытие строк 223-240 (Ошибки стриминга)"""

    # 1. Создаем мок клиента, который при попытке stream() кидает TimeoutError
    mock_net_ctx.client.stream = MagicMock()
    mock_net_ctx.client.stream.return_value.__aenter__.side_effect = TimeoutError(
        "Net down"
    )

    # Функция должна сделать 3 попытки (поспать 3 раза) и взорваться с RequestError
    with pytest.raises(httpx.RequestError, match="Failed to establish stream"):
        async with stream_chunk(mock_net_ctx, "http://test", {}, 60):
            pass

    # Проверяем, что он честно попытался 3 раза уснуть
    assert mock_sleep.call_count == 3
