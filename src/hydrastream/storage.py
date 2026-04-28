from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

from hydrastream.exceptions import (
    FileSizeMismatchError,
    HashMismatchError,
    HydraFileNotFoundError,
    InsufficientSpaceError,
    StateSaveError,
)
from hydrastream.models import File, TypeHash


class LocalStorageManager:
    def __init__(self, output_dir: Path, debug: bool = False) -> None:
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.state_dir = self.output_dir / ".states"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.debug = debug

    def allocate_space(self, filename: str, size: int) -> str | None:
        free_space = shutil.disk_usage(self.output_dir).free
        if free_space < size:
            raise InsufficientSpaceError(
                path=self.output_dir, required=size, free=free_space
            )

        filepath = self.output_dir / filename

        if filepath.is_file():
            filepath = self.get_unique_path(filepath)

        fd = os.open(filepath, os.O_RDWR | os.O_CREAT)
        try:
            if hasattr(os, "posix_fallocate") and size > 0:
                os.posix_fallocate(fd, 0, size)
            else:
                os.ftruncate(fd, size)
        finally:
            os.close(fd)

        if filepath.name != filename:
            return filepath.name
        return None

    def open_file(self, filename: str) -> int:
        filepath = self.output_dir / filename
        return os.open(filepath, os.O_RDWR)

    def write_chunk_data(
        self, fd_or_conn: int, data_bytes: list[bytes], len_data: int, offset: int
    ) -> None:
        """Главный диспетчер записи."""
        if hasattr(os, "pwritev"):
            self._write_posix_vectored(fd_or_conn, data_bytes, len_data, offset)

            # Сброс кэша (только для POSIX)
            if hasattr(os, "posix_fadvise"):
                with contextlib.suppress(Exception):
                    os.posix_fadvise(
                        fd_or_conn, offset, len_data, os.POSIX_FADV_DONTNEED
                    )
        else:
            self._write_windows_merged(fd_or_conn, data_bytes, len_data, offset)

    def _write_posix_vectored(
        self, fd: int, data_bytes: list[bytes], len_data: int, offset: int
    ) -> None:
        """Сложная векторная запись (Zero-Copy) для
        Linux/macOS с обработкой частичной записи."""
        views = [memoryview(b) for b in data_bytes]
        written = 0
        retries = 0

        # Флаг RWF_DSYNC есть только в новых версиях Python/Linux
        flags = os.RWF_DSYNC if hasattr(os, "RWF_DSYNC") else 0

        while written < len_data:
            try:
                if flags:
                    n = os.pwritev(fd, views, offset + written, flags)
                else:
                    n = os.pwritev(fd, views, offset + written)

                if n <= 0:
                    raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

                written += n

                # Если ядро не смогло записать всё за один раз, перестраиваем векторы
                if written < len_data:
                    views = self._rebuild_memoryviews(views, n)

            except OSError as e:
                self._handle_io_retry(e, retries)
                retries += 1

    def _write_windows_merged(
        self, fd: int, data_bytes: list[bytes], len_data: int, offset: int
    ) -> None:
        """Классическая запись склеенным куском для Windows."""
        merged_data = b"".join(data_bytes)
        view = memoryview(merged_data)
        written = 0
        retries = 0

        while written < len_data:
            try:
                n = os.pwrite(fd, view[written:], offset + written)
                if n <= 0:
                    raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
                written += n
            except OSError as e:
                self._handle_io_retry(e, retries)
                retries += 1

    def _rebuild_memoryviews(
        self, views: list[memoryview], bytes_skipped: int
    ) -> list[memoryview]:
        """Утилита для обрезки записанных кусков из массива векторов."""
        new_views: list[memoryview] = []
        skip = bytes_skipped
        for v in views:
            if skip >= len(v):
                skip -= len(v)
            else:
                new_views.append(v[skip:])
                skip = 0
        return new_views

    def _handle_io_retry(self, e: OSError, retries: int) -> None:
        """Обрабатывает системные прерывания (EAGAIN, EINTR)."""
        if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR):
            if retries > 5:
                raise RuntimeError(f"Too many retries on EAGAIN/EINTR: {e}") from e
            time.sleep(0.01 * (2**retries))  # Exponential backoff
        else:
            raise e

    def close_file(self, fd_or_conn: int) -> None:
        with contextlib.suppress(OSError):
            os.close(fd_or_conn)

    def delete_file(self, filename: str) -> None:
        filepath = self.output_dir / filename
        filepath.unlink(missing_ok=True)

    def save_state(self, file_obj: File) -> None:
        filename = file_obj.actual_filename
        path = Path(self.get_state_path(filename))
        temp_dir = path.parent
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
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
            except Exception as e:
                if Path.exists(temp_path):
                    Path.unlink(temp_path)
                raise e
        except OSError as e:
            raise StateSaveError(
                filename=filename, target_path=str(path), reason=str(e)
            ) from e

    def load_state(self, filename: str) -> tuple[File | None, int]:
        p = Path(filename)
        main_name = p.stem  # "GCF_..._genomic.fna"
        last_ext = p.suffix  # ".gz"

        # Экранируем обе части
        safe_main = re.escape(main_name)
        safe_ext = re.escape(last_ext)

        # Паттерн: Название + (число) + расширение + .state.json
        pattern = re.compile(rf"^{safe_main}(?: \((\d+)\))?{safe_ext}\.state\.json$")

        found_states: list[tuple[Path, int]] = []
        for f in self.state_dir.iterdir():
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
            if self.debug:
                raise
            return None, len(found_states)

        if not file:
            return None, len(found_states)

        if (self.output_dir / file.meta.original_filename).is_file():
            return file, len(found_states)
        return None, len(found_states)

    def delete_state(self, filename: str) -> None:
        self.get_state_path(filename).unlink(missing_ok=True)

    def verify_size(self, filename: str, expected_size: int) -> bool:
        file_path = self.output_dir / filename

        if not file_path.is_file():
            raise HydraFileNotFoundError(filename=filename, path=str(file_path))

        actual_size = file_path.stat().st_size

        if expected_size and actual_size != expected_size:
            raise FileSizeMismatchError(
                filename=filename, expected=expected_size, actual=actual_size
            )

        return True

    async def verify_file_hash(
        self, filename: str, expected_checksum: str, algorithm: TypeHash
    ) -> None:
        if not expected_checksum:
            return

        def _compute_hash(algorithm: TypeHash) -> str:
            filepath = self.output_dir / filename
            if not filepath.exists():
                raise HydraFileNotFoundError(filename=filename, path=str(filepath))
            with filepath.open("rb") as f:
                digest = hashlib.file_digest(f, algorithm)
                return digest.hexdigest()

        loop = asyncio.get_running_loop()
        calculated = await loop.run_in_executor(None, _compute_hash, algorithm)

        if calculated != expected_checksum:
            filepath = self.output_dir / filename
            filepath.unlink(missing_ok=True)
            raise HashMismatchError(
                filename=filename,
                algorithm=algorithm,
                expected=expected_checksum,
                actual=calculated,
            )

    def get_unique_path(self, file_path: Path) -> Path:
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

    def get_state_path(self, filename: str) -> Path:
        return self.state_dir / f"{filename}.state.json"
