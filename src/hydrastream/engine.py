# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import heapq
import signal
from collections.abc import AsyncGenerator, Iterable

from hydrastream.dispatcher import run_dispatch_loop
from hydrastream.models import HydraContext
from hydrastream.monitor import done, log, ui_start, ui_stop
from hydrastream.network import close
from hydrastream.producer import chunk_producer
from hydrastream.storage import autosave, save_all_states, verify_stream


async def _stop(ctx: HydraContext, complete: bool = False) -> None:

    if not ctx.is_running:
        return

    ctx.is_running = False

    if not complete:
        await log(
            ctx.ui,
            "Interrupt signal received. Initiating graceful shutdown...",
            status="INTERRUPT",
        )

    if ctx.task_creator:
        ctx.task_creator.cancel()
    if ctx.autosave_task:
        ctx.autosave_task.cancel()
    if ctx.workers:
        for worker in ctx.workers:
            if worker and not worker.done():
                worker.cancel()

    if ctx.stream:
        with contextlib.suppress(asyncio.QueueFull):
            ctx.stream_queue.put_nowait((-1, bytearray()))

    async with ctx.condition:
        ctx.condition.notify_all()


async def stream_all(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, str] | None = None,
) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:

    await ui_start(ctx.ui)
    ctx.stream = True
    if isinstance(links, str):
        links = [links]
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_stop(ctx)))

    ctx.task_creator = asyncio.create_task(
        chunk_producer(ctx, links, expected_checksums)
    )
    ctx.workers = [
        asyncio.create_task(run_dispatch_loop(ctx)) for _ in range(ctx.config.threads)
    ]

    try:
        for _ in links:
            if not ctx.is_running:
                break

            filename = await ctx.file_discovery_queue.get()

            if filename is None:
                continue

            file_gen = _stream_one(ctx, filename)

            yield filename, file_gen
            async with ctx.condition:
                await ctx.condition.wait_for(
                    lambda f=filename: f not in ctx.files or not ctx.is_running
                )

    except asyncio.CancelledError:
        pass

    except Exception as e:
        await log(ctx.ui, f"Runtime Exception in run(): {e}", status="CRITICAL")
        raise

    finally:
        await _stop(ctx, complete=True)

        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(ctx.task_creator, *ctx.workers, return_exceptions=True)

        if (
            ctx.is_running
            and ctx.ui.total_files > 0
            and ctx.ui.total_files == ctx.ui.files_completed
        ):
            await log(
                ctx.ui,
                "All downloads completed successfully!",
                status="SUCCESS",
                progress=True,
            )

        await close(ctx.net)
        await ui_stop(ctx.ui)

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


async def _stream_one(ctx: HydraContext, filename: str) -> AsyncGenerator[bytes]:

    ctx.current_file = filename

    async with ctx.condition:
        ctx.condition.notify_all()

    file_obj = ctx.files[filename]
    total_size = file_obj.meta.content_length
    expected_checksum = file_obj.meta.expected_md5
    md5_hasher = hashlib.md5() if expected_checksum else None

    next_offset = 0
    await log(ctx.ui, f"Streaming: {filename}", status="INFO")
    try:
        while next_offset < total_size:
            if not ctx.is_running:
                break

            if ctx.heap and ctx.heap[0][0] == next_offset:
                _, chunk_data = heapq.heappop(ctx.heap)
                chunk_bytes = bytes(chunk_data)

                async with ctx.condition:
                    ctx.condition.notify()

                if md5_hasher:
                    md5_hasher.update(chunk_bytes)

                yield chunk_bytes

                length = len(chunk_bytes)
                next_offset += length
                continue

            chunk_start, chunk_data = await ctx.stream_queue.get()
            if chunk_start == -1:
                break

            if chunk_start == next_offset:
                chunk_bytes = bytes(chunk_data)

                if md5_hasher:
                    md5_hasher.update(chunk_bytes)

                yield chunk_bytes

                length = len(chunk_bytes)
                next_offset += length
            else:
                heapq.heappush(ctx.heap, (chunk_start, chunk_data))
        else:
            await done(ctx.ui, filename)

            if md5_hasher and expected_checksum:
                try:
                    verify_stream(
                        md5_hasher, expected_checksum, next_offset, total_size
                    )
                    await log(ctx.ui, "MD5 Verified", status="SUCCESS", progress=True)
                except Exception as e:
                    await log(ctx.ui, str(e), status="ERROR")
                    raise

    finally:
        ctx.heap.clear()
        del ctx.files[filename]


async def run_downloads(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, str] | None = None,
) -> None:

    await ui_start(ctx.ui)
    ctx.stream = False
    if isinstance(links, str):
        links = [links]

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_stop(ctx)))
    ctx.task_creator = asyncio.create_task(
        chunk_producer(ctx, links, expected_checksums)
    )
    ctx.autosave_task = asyncio.create_task(autosave(ctx, interval=60))
    ctx.workers = [
        asyncio.create_task(run_dispatch_loop(ctx)) for _ in range(ctx.config.threads)
    ]

    try:
        await ctx.task_creator
        async with ctx.condition:
            await ctx.condition.wait_for(lambda: not (ctx.files and ctx.is_running))

        if (
            ctx.is_running
            and ctx.ui.total_files > 0
            and ctx.ui.total_files == ctx.ui.files_completed
        ):
            await log(
                ctx.ui,
                "All downloads completed successfully!",
                status="SUCCESS",
                progress=True,
            )

    except asyncio.CancelledError:
        pass

    except Exception as e:
        await log(ctx.ui, f"Runtime Exception in run(): {e}", status="CRITICAL")
        raise

    finally:
        await _stop(ctx, complete=True)

        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(
                ctx.task_creator,
                ctx.autosave_task,
                *ctx.workers,
                return_exceptions=True,
            )

        save_all_states(ctx.fs, ctx.files)

        for file_obj in ctx.files.values():
            file_obj.close_fd()

        await close(ctx.net)
        await ui_stop(ctx.ui)

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


async def engine_start(ctx: HydraContext) -> HydraContext:
    await ui_start(ctx.ui)
    return ctx


async def engine_stop(ctx: HydraContext) -> None:
    await _stop(ctx)
    await ui_stop(ctx.ui)
