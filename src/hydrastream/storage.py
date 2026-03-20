# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from _hashlib import HASH
from copy import deepcopy
from pathlib import Path

from hydrastream.monitor import log

from .models import File, HydraContext, StorageState


def get_unique_path(file_path: Path) -> Path:
    if not file_path.is_file():
        return file_path

    stem = file_path.stem
    suffix = file_path.suffix
    directory = file_path.parent

    counter = 1

    while True:
        new_name = f"{stem} ({counter}){suffix}"
        new_path = directory / new_name

        if not new_path.is_file():
            return new_path

        counter += 1


def create_sparse_file(ctx: StorageState, filename: str, size: int) -> str | None:

    free_space = shutil.disk_usage(ctx.out_dir).free
    if free_space < size:
        raise OSError(
            f"Insufficient disk space. "
            f"Required: {size / (1024**2):.2f} MB,"
            f" Available: {free_space / (1024**2):.2f} MB."
        )
    filepath = ctx.out_dir / filename

    if filepath.is_file():
        filepath = get_unique_path(filepath)

    with filepath.open("wb") as f:
        f.truncate(size)

    if filepath.name != filename:
        return filepath.name
    return None


def open_file(ctx: StorageState, filename: str) -> int:

    filepath = ctx.out_dir / filename
    return os.open(filepath, os.O_RDWR)


async def write_chunk_data(fd: int, data: bytearray, offset: int) -> None:

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, os.pwrite, fd, data, offset)


def get_state_path(ctx: StorageState, filename: str) -> Path:
    """Constructs the path for the JSON state file."""
    return ctx.state_dir / f"{filename}.state.json"


def save_state(ctx: StorageState, file_obj: File) -> None:

    path = Path(get_state_path(ctx, file_obj.meta.filename))
    temp_dir = path.parent
    temp_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("wb", dir=temp_dir, delete=False) as tf:
        tf.write(file_obj.to_json())
        tf.flush()
        os.fsync(tf.fileno())
        temp_path = Path(tf.name)

    try:
        Path.replace(temp_path, path)
        if os.name != "nt":
            dir_fd = os.open(str(temp_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except Exception:
        if Path.exists(temp_path):
            Path.unlink(temp_path)
        raise


def save_all_states(ctx: StorageState, files: dict[str, File]) -> None:

    files_snapshot = deepcopy(files)
    for file in files_snapshot.values():
        if not all(c.current_pos > c.end for c in (file.chunks or [])):
            save_state(ctx, file)


async def autosave(ctx: HydraContext, interval: float) -> None:
    loop = asyncio.get_running_loop()
    while ctx.is_running:
        try:
            await asyncio.sleep(interval)
            await loop.run_in_executor(None, save_all_states, ctx.fs, ctx.files)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await log(ctx.ui, f"Auto-save operation failed: {e}", status="ERROR")


def load_state(ctx: StorageState, filename: str) -> tuple[File | None, int]:

    p = Path(filename)
    main_name = p.stem  # "GCF_..._genomic.fna"
    last_ext = p.suffix  # ".gz"

    # Экранируем обе части
    safe_main = re.escape(main_name)
    safe_ext = re.escape(last_ext)

    # Паттерн: Название + (число) + расширение + .state.json
    pattern = re.compile(rf"^{safe_main}(?: \((\d+)\))?{safe_ext}\.state\.json$")

    found_states: list[tuple[Path, int]] = []
    for f in ctx.state_dir.iterdir():
        match = pattern.match(f.name)
        if match:
            # Если скобок нет (оригинал), считаем номер 0
            # Если есть, берем число из первой группы
            counter = int(match.group(1)) if match.group(1) else 0
            found_states.append((f, counter))

    if not found_states:
        return None, 0

    # Сортируем по числу (второй элемент кортежа) и берем самый большой
    state_path, _ = max(found_states, key=lambda x: x[1])

    try:
        with state_path.open("rb") as f:
            content = f.read()
        file = File.from_json(content) if content else None
    except Exception:
        return None, len(found_states)

    if not file:
        return None, len(found_states)

    if (ctx.out_dir / file.meta.filename).is_file():
        return file, len(found_states)

    return None, len(found_states)


def delete_state(ctx: StorageState, filename: str) -> None:

    get_state_path(ctx, filename).unlink(missing_ok=True)


def verify_size(ctx: StorageState, file: File) -> None:

    file_path = ctx.out_dir / file.meta.filename

    if file_path.is_file():
        actual_size = file_path.stat().st_size
        expected_size = file.meta.content_length

        if expected_size and actual_size != expected_size:
            err_msg = (
                f"Size mismatch for {file.meta.filename}: "
                f"Expected {expected_size} bytes, got {actual_size} bytes."
            )
            raise ValueError(err_msg)


async def verify_file_hash(ctx: StorageState, file: File) -> bool | None:
    if file.verified or not file.is_complete:
        return False

    file.verified = True

    if not file.meta.expected_md5:
        return True

    await log(
        ctx.ui,
        f"Verifying MD5 checksum for {file.meta.filename}...",
        status="INFO",
    )

    def _compute_hash() -> str:
        filepath = ctx.out_dir / file.meta.filename
        if not filepath.exists():
            raise OSError(f"File: {file.meta.filename} not found")
        hash_md5 = hashlib.md5()
        with filepath.open("rb") as f:
            for chunk in iter(lambda: f.read(4096 * 1024), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    try:
        loop = asyncio.get_running_loop()
        calculated = await loop.run_in_executor(None, _compute_hash)

        if calculated != file.meta.expected_md5:
            filepath = ctx.out_dir / file.meta.filename
            err_msg = (
                f"CRITICAL: Hash mismatch for {file.meta.filename}!\n"
                f"Expected: {file.meta.expected_md5}\n"
                f"Got:      {calculated}"
            )

            filepath.unlink(missing_ok=True)
            raise ValueError(err_msg)
        await log(
            ctx.ui,
            f"Integrity confirmed: {file.meta.filename}",
            status="SUCCESS",
        )
        return True

    except (ValueError, OSError) as ve:
        await log(ctx.ui, str(ve), status="ERROR")
        raise


def verify_stream(
    md5_hasher: HASH, expected_checksum: str, next_offset: int, total_size: int
) -> None:

    calculated = md5_hasher.hexdigest()
    if calculated != expected_checksum:
        err_msg = (
            f"CRITICAL: Stream Integrity Check Failed!\n"
            f"Expected MD5: {expected_checksum}\n"
            f"Got MD5:      {calculated}"
        )
        raise ValueError(err_msg)

    if next_offset != total_size:
        raise ValueError(
            f"Incomplete stream data! Yielded {next_offset} bytes,"
            f" but expected {total_size} bytes."
        )
