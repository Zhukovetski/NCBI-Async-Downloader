import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Literal

import httpx
import pytest
import respx

from hydrastream.engine import run_downloads, stop, stream_all
from hydrastream.models import HydraConfig, HydraContext
from hydrastream.producer import chunk_producer


@pytest.mark.asyncio
@respx.mock
async def test_add_task_producer(tmp_path: Path) -> None:

    url = "https://fake-ncbi.com/genome.gz"

    respx.head(url).mock(
        return_value=httpx.Response(200, headers={"Content-Length": "500"})
    )
    config = HydraConfig(out_dir=str(tmp_path), quiet=True)
    engine = HydraContext(config=config)

    await chunk_producer(engine, [url], expected_checksums=None)
    assert "genome.gz" in engine.files

    file_obj = engine.files["genome.gz"]
    assert file_obj.meta.content_length == 500

    file_path = engine.fs.out_dir / "genome.gz"
    assert file_path.exists()
    assert file_path.stat().st_size == 500

    assert not engine.chunk_queue.empty()


@pytest.mark.asyncio
@respx.mock
async def test_loader_handles_404_gracefully(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/missing_genome.gz"

    respx.head(url).mock(return_value=httpx.Response(404))

    config = HydraConfig(out_dir=str(tmp_path), quiet=True)
    state = HydraContext(config=config)
    await run_downloads(state, url)

    filename = "missing_genome.gz"

    assert state
    assert filename not in state.files
    assert not (tmp_path / filename).exists()


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_prevents_hang_run(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/huge_file.gz"

    respx.head(url).mock(
        return_value=httpx.Response(200, headers={"Content-Length": "100000000000"})
    )

    async def slow_stream() -> AsyncGenerator[Literal[b"12345"]]:
        while True:
            yield b"12345"
            await asyncio.sleep(1)

    respx.get(url).mock(return_value=httpx.Response(206, content=slow_stream()))

    config = HydraConfig(out_dir=str(tmp_path), quiet=True, threads=2)
    state = HydraContext(config=config)

    run_task = asyncio.create_task(run_downloads(state, url))

    assert state
    await asyncio.sleep(0.5)
    await stop(state)

    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        pytest.fail(
            "CRITICAL ERROR: The program freezes when stopped! Dedlock is in line!"
        )

    assert state.is_running is False


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_prevents_hang_stream(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/huge_stream_file.gz"

    respx.head(url).mock(
        return_value=httpx.Response(200, headers={"Content-Length": "100000000000"})
    )

    async def slow_stream() -> AsyncGenerator[Literal[b"12345"]]:
        while True:
            yield b"12345"
            await asyncio.sleep(1)

    respx.get(url).mock(return_value=httpx.Response(206, content=slow_stream()))

    config = HydraConfig(out_dir=str(tmp_path), quiet=True, threads=2)
    state = HydraContext(config=config)

    async def consume_stream() -> None:
        async for _, file_gen in stream_all(state, [url]):
            async for _ in file_gen:
                pass

    run_task = asyncio.create_task(consume_stream())

    assert state
    await asyncio.sleep(0.5)
    await stop(state)

    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        pytest.fail(
            "CRITICAL ERROR: stream_all freezes when stopped! Dedlock is in line!"
        )

    assert state.is_running is False
