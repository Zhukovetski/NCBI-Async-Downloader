# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import random

import httpx

from hydrastream.models import Chunk, HydraContext
from hydrastream.monitor import done, log, update
from hydrastream.network import stream_chunk
from hydrastream.storage import (
    delete_state,
    open_file,
    verify_file_hash,
    verify_size,
    write_chunk_data,
)


async def file_done(ctx: HydraContext, chunk: Chunk) -> None:
    if ctx.stream:
        return

    filename = chunk.filename
    file_obj = ctx.files[filename]
    if not await verify_file_hash(ctx.fs, file_obj):
        return
    file_obj.close_fd()
    verify_size(ctx.fs, file_obj)
    delete_state(ctx.fs, filename)
    await done(ctx.ui, filename)
    del ctx.files[filename]

    if not ctx.files:
        async with ctx.condition:
            ctx.condition.notify_all()


async def get_chunk(ctx: HydraContext) -> Chunk | None:
    _, chunk = await ctx.chunk_queue.get()
    file_obj = ctx.files.get(chunk.filename)
    if not file_obj or file_obj.is_failed:
        return None

    if ctx.stream and ctx.current_file != chunk.filename:
        async with ctx.condition:
            await ctx.condition.wait_for(
                lambda c=chunk.filename: ctx.current_file == c or not ctx.is_running
            )
    return chunk


async def run_dispatch_loop(ctx: HydraContext) -> None:
    while ctx.is_running:
        chunk = None
        try:
            chunk = await get_chunk(ctx)
            if chunk is None:
                continue
            await process_chunk(ctx, chunk)
            await file_done(ctx, chunk)

        except asyncio.CancelledError:
            break

        except httpx.HTTPStatusError as e:
            if chunk and e.response.status_code in {400, 401, 403, 404, 410, 416}:
                await log(
                    ctx.ui,
                    f"Chunk for {chunk.filename} failed permanently "
                    f"(HTTP {e.response.status_code}).",
                    status="ERROR",
                )
                ctx.files[chunk.filename].is_failed = True
            elif chunk and ctx.is_running:
                await asyncio.sleep(random.uniform(0.5, 2.0))
                await ctx.chunk_queue.put((-1, chunk))

        except (httpx.RequestError, TimeoutError):
            if ctx.is_running and chunk:
                await asyncio.sleep(random.uniform(1.0, 3.0))
                await ctx.chunk_queue.put((-1, chunk))

        except Exception as e:
            await log(ctx.ui, f"Critical Worker Exception: {e!r}", status="CRITICAL")
            raise
        finally:
            ctx.chunk_queue.task_done()


async def disk_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str]
) -> None:
    buffer = bytearray()
    fd = ctx.files[chunk.filename].fd
    if fd is None:
        fd = open_file(ctx.fs, chunk.filename)
    buffer_size = 1_048_576
    async with stream_chunk(
        ctx.net,
        ctx.files[chunk.filename].meta.url,
        headers=headers,
        chunk_timeout=ctx.config.chunk_timeout,
    ) as r:
        try:
            async for data in r.aiter_bytes():
                buffer.extend(data)
                update(ctx.ui, chunk.filename, len(data))
                if len(buffer) >= buffer_size:
                    await write_chunk_data(fd, buffer, chunk.current_pos)
                    chunk.current_pos += len(buffer)
                    buffer = bytearray()

        finally:
            if buffer:
                await write_chunk_data(fd, buffer, chunk.current_pos)
                chunk.current_pos += len(buffer)


async def stream_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str]
) -> None:
    buffer = bytearray()
    async with stream_chunk(
        ctx.net,
        ctx.files[chunk.filename].meta.url,
        headers=headers,
        chunk_timeout=ctx.config.chunk_timeout,
    ) as r:
        try:
            async for data in r.aiter_bytes():
                buffer.extend(data)
                update(ctx.ui, chunk.filename, len(data))
                if len(ctx.heap) >= ctx.heap_size:
                    async with ctx.condition:
                        await ctx.condition.wait_for(
                            lambda: len(ctx.heap) <= ctx.heap_size
                        )
            await ctx.stream_queue.put((chunk.current_pos, buffer))
            chunk.current_pos = chunk.current_pos + len(buffer)
            buffer = bytearray()
        except asyncio.CancelledError:
            raise
        except Exception:
            if ctx.stream and buffer:
                with contextlib.suppress(asyncio.QueueFull):
                    ctx.stream_queue.put_nowait((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
            raise


async def process_chunk(ctx: HydraContext, chunk: Chunk) -> None:
    if chunk.current_pos > chunk.end:
        return
    headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
    if not ctx.stream:
        await disk_process_chunk(ctx, chunk, headers)
    else:
        await stream_process_chunk(ctx, chunk, headers)
