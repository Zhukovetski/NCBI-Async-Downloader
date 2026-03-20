# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import re
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer
from pytest_mock import MockerFixture
from werkzeug.wrappers import Request, Response

from hydrastream.facade import HydraClient

# --- ТЕСТОВЫЕ ДАННЫЕ ---
DUMMY_DATA = b"0123456789" * 100  # 1000 байт
DUMMY_MD5 = hashlib.md5(DUMMY_DATA).hexdigest()


def range_request_handler(request: Request) -> Response:
    """Универсальный обработчик для любых URL на нашем фейковом сервере."""
    if request.method == "HEAD":
        return Response(
            status=200,
            headers={"Content-Length": str(len(DUMMY_DATA)), "Accept-Ranges": "bytes"},
        )

    range_header = request.headers.get("Range")
    if range_header and range_header.startswith("bytes="):
        byte_range = range_header.replace("bytes=", "")
        start_str, end_str = byte_range.split("-")
        start, end = int(start_str), int(end_str)

        chunk = DUMMY_DATA[start : end + 1]

        return Response(
            chunk,
            status=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/{len(DUMMY_DATA)}",
                "Content-Length": str(len(chunk)),
            },
        )
    return Response(
        DUMMY_DATA, status=200, headers={"Content-Length": str(len(DUMMY_DATA))}
    )


# ==========================================
# 1. ТЕСТЫ ОДИНОЧНОЙ ЗАГРУЗКИ (База)
# ==========================================


@pytest.mark.asyncio
async def test_e2e_disk_single(
    tmp_path: Path, httpserver: HTTPServer, mocker: MockerFixture
) -> None:
    mocker.patch("hydrastream.constants.MIN_CHUNK", 100)
    # Настраиваем сервер отвечать на любые пути
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(
        range_request_handler
    )
    url = httpserver.url_for("/genome1.bin")

    async with HydraClient(
        threads=4, no_ui=True, quiet=True, out_dir=str(tmp_path)
    ) as client:
        await asyncio.wait_for(
            client.run(links=[url], expected_checksums={url: DUMMY_MD5}), timeout=10.0
        )

    assert (tmp_path / "genome1.bin").read_bytes() == DUMMY_DATA
    assert not (tmp_path / ".states" / "genome1.bin.state.json").exists()


# ==========================================
# 2. ТЕСТЫ МНОЖЕСТВЕННОЙ ЗАГРУЗКИ
# ==========================================


@pytest.mark.asyncio
async def test_e2e_disk_multiple(
    tmp_path: Path, httpserver: HTTPServer, mocker: MockerFixture
) -> None:
    """Доказывает, что HydraStream переваривает список URL и не путает их чанки."""
    mocker.patch("hydrastream.constants.MIN_CHUNK", 100)
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(
        range_request_handler
    )

    urls = [httpserver.url_for(f"/multi_{i}.bin") for i in range(3)]
    checksums = {u: DUMMY_MD5 for u in urls}

    async with HydraClient(
        threads=6, no_ui=True, quiet=True, out_dir=str(tmp_path)
    ) as client:
        await asyncio.wait_for(
            client.run(links=urls, expected_checksums=checksums), timeout=15.0
        )

    for i in range(3):
        assert (tmp_path / f"multi_{i}.bin").read_bytes() == DUMMY_DATA
        assert not (tmp_path / ".states" / f"multi_{i}.bin.state.json").exists()


@pytest.mark.asyncio
async def test_e2e_stream_multiple(
    tmp_path: Path, httpserver: HTTPServer, mocker: MockerFixture
) -> None:
    """Доказывает, что потоки выдаются строго последовательно файл за файлом."""
    mocker.patch("hydrastream.constants.MIN_CHUNK", 100)
    mocker.patch("hydrastream.constants.STREAM_CHUNK_SIZE", 100)
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(
        range_request_handler
    )

    urls = [httpserver.url_for(f"/stream_{i}.bin") for i in range(2)]
    checksums = {u: DUMMY_MD5 for u in urls}

    results: dict[str, bytes] = {}

    async with HydraClient(
        threads=4, no_ui=True, quiet=True, out_dir=str(tmp_path)
    ) as client:

        async def consume() -> None:
            async for filename, stream_gen in client.stream(
                links=urls, expected_checksums=checksums
            ):
                buf = bytearray()
                async for chunk in stream_gen:
                    buf.extend(chunk)
                results[filename] = bytes(buf)

        await asyncio.wait_for(consume(), timeout=15.0)

    assert len(results) == 2
    assert results["stream_0.bin"] == DUMMY_DATA
    assert results["stream_1.bin"] == DUMMY_DATA
    assert not (tmp_path / "stream_0.bin").exists()  # На диск ничего не упало!


# ==========================================
# 3. ТЕСТ ПРЕРЫВАНИЯ И СОХРАНЕНИЯ СТЕЙТА
# ==========================================


@pytest.mark.asyncio
async def test_e2e_interruption_saves_state(
    tmp_path: Path, httpserver: HTTPServer, mocker: MockerFixture
) -> None:
    """
    Имитирует Ctrl+C (отмену задачи).
    Проверяет, что воркеры корректно умирают, а стейт-файл записывается на диск.
    """
    mocker.patch("hydrastream.constants.MIN_CHUNK", 100)
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(
        range_request_handler
    )
    url = httpserver.url_for("/interrupt.bin")

    # Искусственно замедляем запись на диск, чтобы успеть отменить задачу в
    # процессе скачивания!
    async def slow_write(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(0.5)

    # Мокаем функцию записи внутри диспетчера
    mocker.patch("hydrastream.dispatcher.write_chunk_data", side_effect=slow_write)

    async with HydraClient(
        threads=2, no_ui=True, quiet=True, out_dir=str(tmp_path)
    ) as client:
        # Запускаем загрузку как фоновую задачу
        run_task = asyncio.create_task(client.run([url]))

        # Даем ей 0.2 секунды (хватит чтобы начать качать первый чанк и зависнуть
        # на slow_write)
        await asyncio.sleep(0.2)

        # БЬЕМ ПО ТОРМОЗАМ (Имитация Ctrl+C)
        run_task.cancel()

        # Ждем завершения с подавлением ошибки отмены
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(run_task, timeout=5.0)

    # ГЛАВНАЯ ПРОВЕРКА: Файл .state.json должен был сохраниться в папку .states!
    state_file = tmp_path / ".states" / "interrupt.bin.state.json"
    assert state_file.exists(), "Стейт не сохранился при аварийном завершении!"

    # Также проверяем, что сам бинарник (sparse file) был создан
    assert (tmp_path / "interrupt.bin").exists()
