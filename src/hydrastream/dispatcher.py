# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import math
import os
import random
import sys
import time
from typing import cast

from curl_cffi import CurlOpt, Response
from curl_cffi.requests import RequestsError

from .models import Chunk, HydraContext
from .monitor import done, log, update
from .network import stream_chunk, try_scale_up


async def telemetry_worker(ctx: HydraContext) -> None:
    """Фоновый инспектор. Управляет скоростью и Авто-тюнингом воркеров."""

    ctx.ui.last_checkpoint_time = time.monotonic()
    smoothed_speed = 0.0
    prev_speed = 0.0
    current_limit = 2

    # Безопасные границы для адаптивного окна
    TAU = 2.0
    MIN_WINDOW = 1024  # 1 КБ (чтобы не сжечь CPU)

    while True:  # Проверяем глобальный флаг
        # 1. Ждем пульса от Монитора
        await ctx.ui.checkpoint_event.wait()
        ctx.ui.checkpoint_event.clear()

        now = time.monotonic()
        elapsed = min(1, now - ctx.ui.last_checkpoint_time)

        if elapsed <= 0:
            continue  # Защита от деления на ноль при сверхбыстрых всплесках

        # =========================================================
        # ФАЗА 1: ЛОГИКА ОГРАНИЧИТЕЛЯ СКОРОСТИ (Global Valve)
        # =========================================================
        if ctx.ui.speed_limit:
            target_time = ctx.ui.bytes_to_check / ctx.ui.speed_limit

            if elapsed < target_time:
                sleep_duration = target_time - elapsed

                # ЗАКРЫВАЕМ ВЕНТИЛЬ
                for r in ctx.active_stream:
                    if r.curl is not None:
                        r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 1)
                await asyncio.sleep(sleep_duration)
                # ОТКРЫВАЕМ ВЕНТИЛЬ
                for r in ctx.active_stream:
                    if r.curl is not None:
                        r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 0)
                # Важно: мы спали! Нужно пересчитать 'now' и 'elapsed'
                # для Авто-тюнера, чтобы он считал РЕАЛЬНУЮ скорость с учетом паузы.
                now = time.monotonic()
                elapsed = now - ctx.ui.last_checkpoint_time

                continue

        # =========================================================
        # ФАЗА 2: АВТО-ТЮНЕР (AIMD Hill Climbing)
        # =========================================================
        speed_now = ctx.ui.bytes_to_check / elapsed

        if smoothed_speed == 0.0:
            smoothed_speed = speed_now
        else:
            # Магия: вычисляем вес нового замера на основе физического времени
            alpha = 1.0 - math.exp(-elapsed / TAU)
            smoothed_speed = (alpha * speed_now) + ((1.0 - alpha) * smoothed_speed)

        coef = 1 / speed_now**0.2
        # Пересчитываем, когда мы хотим проснуться в следующий раз
        new_bytes_to_check = int(ctx.ui.bytes_to_check * (1 - coef + elapsed))

        ctx.ui.bytes_to_check = max(MIN_WINDOW, new_bytes_to_check)
        # Анализируем результат нашего предыдущего шага
        if smoothed_speed > prev_speed * 1.05:
            prev_speed = smoothed_speed
            # Скорость выросла! Добавляем воркеров (если не уперлись в лимит юзера)
            if current_limit < ctx.config.threads:
                current_limit += 1
                await log(
                    ctx.ui,
                    f"Speed increased to {speed_now / 1024**2:.1f} MB/s. Scaling up to {current_limit} workers.",
                    status="INFO",
                    throttle_key="scale_up",
                    throttle_sec=5.0,
                )

        elif smoothed_speed < prev_speed * 0.95:
            prev_speed = smoothed_speed
            # Скорость резко упала (сеть забилась). Скидываем воркеров.
            if current_limit > 2:
                current_limit -= 1
                await log(
                    ctx.ui,
                    f"Network congested. Scaling down to {current_limit} workers.",
                    status="WARNING",
                    throttle_key="scale_down",
                    throttle_sec=5.0,
                )

        # Применяем лимит
        if ctx.dynamic_limit != current_limit:
            ctx.dynamic_limit = current_limit
            # Будим диспетчера только если лимит изменился
            async with ctx.dynamic_limit_cond:
                ctx.dynamic_limit_cond.notify_all()

        # Фиксируем время для следующего круга
        ctx.ui.last_checkpoint_time = time.monotonic()


async def file_done(ctx: HydraContext, chunk: Chunk) -> None:
    if ctx.stream:
        return

    filename = chunk.file.meta.filename
    file_obj = chunk.file
    try:
        if chunk.file.meta.content_length:
            if chunk.file.verified or not chunk.file.is_complete:
                return
            chunk.file.verified = True
            if not ctx.fs.verify_size(filename, file_obj.meta.content_length):
                return

        if file_obj.meta.expected_checksum:
            await log(
                ctx.ui,
                f"Verifying Hash checksum for {chunk.file.meta.filename}...",
                status="INFO",
            )
            await ctx.fs.verify_file_hash(
                file_obj.meta.filename,
                file_obj.meta.expected_checksum.value,
                file_obj.meta.expected_checksum.algorithm,
            )
            await log(
                ctx.ui,
                f"Integrity confirmed: {chunk.file.meta.filename}",
                status="SUCCESS",
            )

    except (ValueError, OSError) as ve:
        await log(ctx.ui, str(ve), status="ERROR")
        raise

    ctx.fs.close_file(fd_or_conn=file_obj.fd)
    ctx.fs.delete_state(filename)
    await done(ctx.ui, filename)
    del ctx.files[chunk.file.meta.id]
    ctx.current_files_id.remove(chunk.file.meta.id)
    async with ctx.current_files_cond:
        ctx.current_files_cond.notify()
    if not ctx.files:
        ctx.all_complete_event.set()


async def get_chunk(ctx: HydraContext) -> Chunk | None:
    if ctx.stream:
        _, chunk = await ctx.chunk_queue.get()
    else:
        chunk, _ = await ctx.chunk_queue.get()
        chunk = cast(Chunk, chunk)
    if _ == sys.maxsize:
        raise asyncio.CancelledError
    file_obj = chunk.file
    if not file_obj or file_obj.is_failed:
        return None

    if ctx.stream:
        async with ctx.chunk_from_future_cond:
            await ctx.chunk_from_future_cond.wait_for(
                lambda: (
                    ctx.next_offset + ctx.config.STREAM_BUFFER_SIZE >= chunk.current_pos
                )
            )
    return chunk


async def download_worker(ctx: HydraContext, worker_id: int) -> None:
    while True:
        try:
            if worker_id >= ctx.dynamic_limit:
                async with ctx.dynamic_limit_cond:
                    await ctx.dynamic_limit_cond.wait_for(
                        lambda: worker_id < ctx.dynamic_limit
                    )
            chunk = None

            chunk = await get_chunk(ctx)
            if chunk is None:
                continue
            await process_chunk(ctx, chunk, worker_id)
            await file_done(ctx, chunk)

        except ValueError:
            if chunk:
                await ctx.chunk_queue.put((-1, chunk))

        except asyncio.CancelledError:
            break

        except RequestsError as e:
            if chunk:
                response = e.response  # type: ignore

                if isinstance(response, Response):
                    # Теперь Pyright ВИДИТ, что это Response, и даст автодополнение
                    status = response.status_code

                    if status in {400, 401, 403, 404, 410, 416}:
                        await log(
                            ctx.ui,
                            f"Chunk for {chunk.file.meta.filename} failed permanently "
                            f"(HTTP {status}).",
                            status="ERROR",
                        )
                        chunk.file.is_failed = True
                        ctx.fs.delete_file(chunk.file.meta.filename)

                    # Остальные ошибки сервера (5xx, 429) — пробуем перекинуть чанк
                    # в очередь
                    else:
                        await requeue_chunk(ctx, chunk, delay_range=(0.5, 2.0))
                # 2. Если ответа нет (Сетевая ошибка / CurlError / Timeout)
                else:
                    await requeue_chunk(ctx, chunk)

            ctx.dynamic_limit = max(ctx.dynamic_limit - 1, 1)
            async with ctx.dynamic_limit_cond:
                ctx.dynamic_limit_cond.notify_all()

        except TimeoutError:
            if chunk:
                await requeue_chunk(ctx, chunk)

        except Exception as e:
            await log(ctx.ui, f"Critical Worker Exception: {e!r}", status="CRITICAL")
            raise


async def requeue_chunk(
    ctx: HydraContext,
    chunk: Chunk,
    delay_range: tuple[float, float] = (1.0, 3.0),
) -> None:
    if not chunk:
        return

    file_obj = chunk.file
    supports_ranges = file_obj.meta.supports_ranges

    if not supports_ranges:
        if ctx.stream:
            err_msg = (
                f"Stream interrupted for {chunk.file.meta.filename}. "
                f"Server does not support partial downloads (Range requests). "
                f"Cannot resume stream. Aborting."
            )
            await log(ctx.ui, err_msg, status="CRITICAL")

            file_obj.is_failed = True
            return

        await log(
            ctx.ui,
            f"Connection dropped for {chunk.file.meta.filename}. "
            f"Server does not support resume. Restarting download from 0 bytes.",
            status="WARNING",
        )

        downloaded_so_far = chunk.current_pos - chunk.start
        if downloaded_so_far > 0:
            update(ctx.ui, chunk.file.meta.filename, -downloaded_so_far)

        chunk.current_pos = chunk.start

        fd = file_obj.fd
        if fd is not None:
            loop = asyncio.get_running_loop()
            # truncate(0) обрезает файл до 0 байт
            await loop.run_in_executor(None, os.ftruncate, fd, 0)

            # Если изначально размер был известен, снова выделяем место
            if file_obj.meta.content_length > 0:
                await loop.run_in_executor(
                    None, os.ftruncate, fd, file_obj.meta.content_length
                )
    await ctx.chunk_queue.put((-1, chunk))
    delay = random.uniform(*delay_range)
    await asyncio.sleep(delay)


async def disk_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str] | None, worker_id: int
) -> None:
    buffer = bytearray()
    fd = chunk.file.fd
    if fd is None:
        fd = ctx.fs.open_file(chunk.file.meta.filename)
    buffer_size = 1_048_576
    async with stream_chunk(
        ctx.net,
        chunk.file.meta.url,
        headers=headers,
    ) as r:
        ctx.active_stream.add(r)
        try:
            async for data in r.aiter_content(chunk_size=131072):  # type: ignore
                data = cast(bytes, data)
                buffer.extend(data)
                update(ctx.ui, chunk.file.meta.filename, len(data))
                if len(buffer) >= buffer_size:
                    await ctx.fs.write_chunk_data(fd, buffer, chunk.current_pos)
                    chunk.current_pos += len(buffer)
                    buffer = bytearray()
                    if random.random() < 0.1:
                        await try_scale_up(ctx.net.rate_limiter)
                if ctx.dynamic_limit <= worker_id:
                    raise ValueError

        finally:
            ctx.active_stream.remove(r)
            if buffer:
                await ctx.fs.write_chunk_data(fd, buffer, chunk.current_pos)
                chunk.current_pos += len(buffer)


async def stream_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str] | None, worker_id: int
) -> None:
    buffer = bytearray()
    async with stream_chunk(
        ctx.net,
        chunk.file.meta.url,
        headers=headers,
    ) as r:
        ctx.active_stream.add(r)
        try:
            async for data in r.aiter_content(chunk_size=131072):  # type: ignore
                data = cast(bytes, data)
                buffer.extend(data)
                update(ctx.ui, chunk.file.meta.filename, len(data))
                if len(buffer) > ctx.config.STREAM_CHUNK_SIZE:
                    await ctx.stream_queue.put((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
                    buffer = bytearray()
                if ctx.dynamic_limit <= worker_id:
                    raise ValueError
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
        finally:
            ctx.active_stream.remove(r)


async def process_chunk(ctx: HydraContext, chunk: Chunk, worker_id: int) -> None:
    if chunk.current_pos > chunk.end:
        return
    if chunk.file.meta.supports_ranges:
        headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
    else:
        headers = None
    if not ctx.stream:
        await disk_process_chunk(ctx, chunk, headers, worker_id)
    else:
        await stream_process_chunk(ctx, chunk, headers, worker_id)
