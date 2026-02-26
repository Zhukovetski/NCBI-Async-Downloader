# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import hashlib
import heapq
import os
import signal
from collections.abc import AsyncGenerator, Iterable
from pathlib import Path
from types import TracebackType
from typing import Self

import httpx
from aiolimiter import AsyncLimiter

from .models import Chunk, File
from .monitor import ProgressMonitor


class NCBILoader:
    _queue: asyncio.Queue[tuple[int, Chunk]]
    _stream_queue: asyncio.Queue[tuple[int, bytearray]]
    workers: list[asyncio.Task[None]]

    def __init__(
        self,
        threads: int = 3,
        silent: bool = False,
        output_dir: str = "download",
        stream_buffer_size: int | None = None,
        timeout: int | float = 10.0,
        follow_redirects: bool = True,
        http2: bool = False,
        verify: bool = False,
    ) -> None:

        self._max_conns = threads
        self._monitor = None
        self._stream = None
        self._stream_buffer_size = stream_buffer_size
        self.MIN_CHUNK = 5 * 1024 * 1024  # 5 MB минимум
        if not silent:
            self._monitor = ProgressMonitor()
        self._limiter = AsyncLimiter(threads * 2, 1)
        self._semaphore = asyncio.Semaphore(threads)
        self._timeout = httpx.Timeout(timeout, read=5.0)
        self.client = httpx.AsyncClient(
            headers={"Accept-Encoding": "identity"},
            timeout=self._timeout,
            follow_redirects=follow_redirects,
            verify=verify,
            http2=http2,
        )

        try:
            self.output_dir = Path(output_dir).expanduser().resolve()
            self.state_dir = self.output_dir / ".states"

            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.state_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(f"Ошибка: Не удалось использовать путь {output_dir}. Причина: {e}")
            raise

        self._queue = asyncio.PriorityQueue(maxsize=threads * 2)
        self._stream_queue = asyncio.Queue(
            maxsize=(
                self._max_conns * 2
                if not stream_buffer_size
                else stream_buffer_size // self.MIN_CHUNK
            )
        )
        self.files: dict[str, File] = {}
        self.remaining: list[Chunk] = []
        self.heap: list[tuple[int, bytearray]] = []
        self.condition = asyncio.Condition()
        self.is_running = True
        self.workers = []
        self.task_creator = None
        self.autosave_task = None

    def _get_state_path(self, filename: str) -> Path:
        return self.state_dir / f"{filename}.state.json"

    def _get_file_path(self, filename: str) -> Path:
        return self.output_dir / filename

    def _save_all_states(self) -> None:
        if self._monitor:
            self._monitor.log("[yellow]Сохранение состояния...[/]")

        for filename, file in list(self.files.items()):
            if not all(c.current_pos > c.end for c in (file.chunks or [])):
                self._save_state(filename)

    async def _autosave(self) -> None:
        while self.is_running:
            try:
                await asyncio.sleep(60)
                self._save_all_states()
            except asyncio.CancelledError:
                break

    def _save_state(self, filename: str) -> None:

        if (file_obj := self.files.get(filename)) is None:
            return
        try:
            path = self._get_state_path(filename)
            path.write_bytes(file_obj.to_json())

        except Exception as e:
            if self._monitor:
                self._monitor.log(f"[red]Ошибка сохранения {filename}: {e}[/]")

    def _verify_file_hash(self, filename: str) -> None:
        """Синхронный метод для запуска в экзекуторе"""
        file_obj = self.files.get(filename)
        if not file_obj or not file_obj.expected_md5:
            return

        filepath = self._get_file_path(filename)
        if not filepath.exists():
            return

        if self._monitor:
            self._monitor.log(f"[*] Проверка MD5 для {filename}...")

        # Считаем MD5
        hash_md5 = hashlib.md5()
        with filepath.open("rb") as f:
            for chunk in iter(lambda: f.read(4096 * 1024), b""):
                hash_md5.update(chunk)

        calculated = hash_md5.hexdigest()

        if calculated != file_obj.expected_md5:
            err_msg = (
                f"CRITICAL: Hash mismatch for {filename}!\n"
                f"Expected: {file_obj.expected_md5}\n"
                f"Got:      {calculated}"
            )
            if self._monitor:
                self._monitor.log(f"[bold red]{err_msg}[/]")
            # Можно удалить битый файл
            # os.remove(filepath)
            # И пометить в file_obj, что он битый, чтобы run() выбросил ошибку в конце
        else:
            if self._monitor:
                self._monitor.log(f"[green]MD5 OK: {filename}[/]")
            file_obj.verified = True
            # Вот тут можно покрасить бар в зеленый
            if self._monitor:
                self._monitor.done(filename)

    async def _add_task_producer(
        self, links: Iterable[str], expected_checksums: dict[str, str] | None = None
    ) -> None:
        """Продюсер: Нарезает файлы и кидает в очередь"""
        checksums_map = (
            expected_checksums if isinstance(expected_checksums, dict) else {}
        )
        for url in links:
            if not self.is_running:
                break

            md5_val = checksums_map.get(url)

            try:
                filename = url.rsplit("/", 1)[1]
                file = None
                state_path = self._get_state_path(filename)
                total_size = 0

                async with self._limiter:
                    resp = await self.client.head(url)
                    if resp.status_code >= 400:
                        if self._monitor:
                            self._monitor.log(
                                f"[red]Ошибка {resp.status_code} для {filename}[/]"
                            )
                        continue  # Пропускаем этот файл
                    total_size = int(resp.headers.get("Content-Length", 0))
                # 1. Попытка восстановления
                if not self._stream:
                    if state_path.is_file():
                        try:
                            with state_path.open("rb") as f:
                                content = f.read()
                                if content:
                                    file = File.from_json(content)
                        except Exception as e:
                            if self._monitor:
                                self._monitor.log(
                                    f"[!] Битая история {filename}. Ошибка: {e}"
                                )
                            file = None

                    elif self._get_file_path(filename).is_file():
                        if self._monitor:
                            self._monitor.log(f"[red]{filename} уже существует![/]")
                        continue

                # 2. Если не восстановили - создаем с нуля

                if not file:
                    # Sparse file creation
                    if not self._stream:
                        with self._get_file_path(filename).open("wb") as f:
                            f.truncate(total_size)

                    # Логика нарезки
                    parts = self._max_conns
                    chunk_size = (
                        max(total_size // parts, self.MIN_CHUNK)
                        if not self._stream
                        else self.MIN_CHUNK
                    )

                    file = File(
                        filename=filename,
                        url=url,
                        content_length=total_size,
                        chunk_size=chunk_size,
                        expected_md5=md5_val,
                    )
                # 3. Регистрируем в UI и памяти
                self.files[filename] = file
                chunks = file.chunks or []
                if self._monitor:
                    # Важно: добавляем total_size
                    # Вычисляем его из чанков (на случай восстановления)

                    self._monitor.add_file(filename, total_size)

                    downloaded = sum(c.uploaded for c in chunks)
                    if downloaded > 0:
                        self._monitor.update(filename, downloaded)

                # 4. Кидаем в очередь (самое важное)
                for c in chunks:
                    if not self.is_running:
                        break
                    async with self.condition:
                        await self.condition.wait_for(lambda: len(self.heap) <= 20)
                    if c.current_pos <= c.end:
                        await self._queue.put((10, c))
                if self._stream:
                    async with self.condition:
                        await self.condition.wait_for(
                            lambda f=filename: f not in self.files
                        )

            except Exception as e:
                if self._monitor:
                    self._monitor.log(f"[red]Ошибка обработки {url}: {e}[/]")
                    raise

    async def _download_worker(self) -> None:
        while self.is_running:
            _, chunk = await self._queue.get()
            try:
                await self._process_chunk(chunk)
                # --- ЛОГИКА МОМЕНТАЛЬНОЙ ПРОВЕРКИ ---
                if not self._stream:  # Только для режима RUN (диск)
                    file_obj = self.files.get(chunk.filename)
                    # Если файл готов И у него есть хеш для проверки И мы его еще не проверяли
                    if (
                        file_obj
                        and file_obj.is_complete
                        and file_obj.expected_md5
                        and not file_obj.verified
                    ):
                        # Ставим флаг, чтобы другой воркер не запустил проверку повторно
                        file_obj.verified = True

                        # Запускаем проверку в фоне!
                        self._verify_file_hash(chunk.filename)
                # ------------------------------------
            except asyncio.CancelledError:
                break
            except Exception:
                if self.is_running:
                    # Мягкий ретрай
                    await asyncio.sleep(2)
                    await self._queue.put((1, chunk))
            finally:
                self._queue.task_done()

    async def _process_chunk(self, chunk: Chunk) -> None:
        async with self._semaphore:
            downloaded_in_this_attempt = 0
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
            try:
                async with self.client.stream(
                    "GET", self.files[chunk.filename].url, headers=headers
                ) as r:
                    if r.status_code >= 400:
                        raise Exception(f"HTTP {r.status_code}")

                    buffer = bytearray()
                    if not self._stream:
                        fd = os.open(self._get_file_path(chunk.filename), os.O_RDWR)
                        buffer_size = 1024 * 1024  # 1 MB
                        try:
                            async for data in r.aiter_bytes():
                                if not self.is_running:
                                    break

                                downloaded_in_this_attempt += len(data)
                                buffer.extend(data)
                                if self._monitor:
                                    self._monitor.update(chunk.filename, len(data))
                                if len(buffer) >= buffer_size:
                                    await self._write_buffer(fd, buffer, chunk)
                                    buffer = bytearray()
                            if buffer:
                                await self._write_buffer(fd, buffer, chunk)

                        except asyncio.CancelledError:
                            if not self._stream:
                                await self._write_buffer(fd, buffer, chunk)
                        finally:
                            os.close(fd)
                    else:
                        async for data in r.aiter_bytes():
                            if not self.is_running:
                                break
                            downloaded_in_this_attempt += len(data)
                            buffer.extend(data)
                            if self._monitor:
                                self._monitor.update(chunk.filename, len(data))
                        await self._stream_queue.put((chunk.start, buffer))
            except Exception:
                if self._monitor and downloaded_in_this_attempt:
                    self._monitor.update(chunk.filename, -downloaded_in_this_attempt)
                raise

    async def _write_buffer(self, fd: int, data: bytearray, chunk: Chunk) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.pwrite, fd, data, chunk.current_pos)
        chunk.current_pos += len(data)

    def stop(self) -> None:
        """Этот метод вызывается по Ctrl+C"""

        if not self.is_running:
            return  # Защита от двойного вызова
        self.is_running = False

        if self._monitor:
            self._monitor.log("[yellow]Получен сигнал остановки...[/]")
        # 1. Останавливаем Продюсеров (чтобы не кидали новое)
        if self.task_creator:
            self.task_creator.cancel()
        if self.autosave_task:
            self.autosave_task.cancel()

        # 2. Отменяем Воркеров (они выйдут из while и вызовут task_done для своих задач)
        for worker in (
            self.workers
        ):  # Предполагаем, что self.workers доступен (сохрани его в self)
            if worker and not worker.done():
                worker.cancel()

        # 3. Самое важное: РАЗБЛОКИРОВКА RUN
        # Метод run висит на await self.queue.join().
        # Он выйдет только если счетчик очереди станет 0.
        # Воркеры погасят свои задачи при выходе.
        # А нам нужно погасить "сирот" (то, что осталось в очереди).
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

        # Всё. Мы почистили очередь и пнули воркеров.
        # Теперь queue.join() в методе run() разблокируется (или почти сразу).
        # И управление перейдет в блок finally метода run().

    async def stream_all(
        self,
        links: Iterable[str] | str,
        expected_checksums: dict[str, str] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:

        self._stream = True
        if isinstance(links, str):
            links = [links]
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

        self.task_creator = asyncio.create_task(
            self._add_task_producer(links, expected_checksums)
        )
        self.workers = [
            asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)
        ]

        try:
            # 2. MAIN LOOP (Обработка файлов по очереди)
            # Мы не итерируемся по links здесь, потому что producer уже накидал их в File state
            # Нам нужно знать порядок. Либо producer сохраняет порядок в отдельный список,
            # либо мы итерируемся по links снова.
            for url in links:
                if not self.is_running:
                    break
                filename = url.rsplit("/", 1)[1]

                # Ждем, пока файл появится в системе (Producer его обработает)
                # Тут нужна синхронизация, если Producer отстает.
                while filename not in self.files:
                    if self.task_creator.done():
                        # Если продюсер умер, а файла нет - беда
                        break
                    await asyncio.sleep(0.1)

                if filename not in self.files:
                    continue

                # Создаем внутренний генератор для ОДНОГО файла
                # Он предполагает, что воркеры уже работают
                file_gen = self._stream_one(filename)

                yield filename, file_gen
                async with self.condition:
                    await self.condition.wait_for(
                        lambda f=filename: f not in self.files or not self.is_running
                    )

        except asyncio.CancelledError:
            pass

        finally:
            # 3. TEARDOWN (Очистка)
            if self._monitor and self.is_running:
                self._monitor.log("[green]Все загрузки завершены![/]")
            self.stop()  # Флаг вниз

            if self.task_creator:
                self.task_creator.cancel()

            for w in self.workers:
                w.cancel()

            # Ждем пока все умрут
            await asyncio.gather(*self.workers, return_exceptions=True)
            if self.task_creator:
                await asyncio.gather(self.task_creator, return_exceptions=True)

            # Чистим очереди
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()

            # Закрываем клиент
            await self.client.aclose()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def _stream_one(self, filename: str) -> AsyncGenerator[bytes]:
        file_obj = self.files[filename]
        total_size = file_obj.content_length
        expected_checksum = file_obj.expected_md5
        md5_hasher = hashlib.md5() if expected_checksum else None
        next_offset = 0
        # Куча для хранения чанков, пришедших раньше времени
        try:
            while next_offset < total_size:
                # 1. Если в куче уже есть следующий кусок - отдаем его
                if self.heap and self.heap[0][0] == next_offset:
                    _, chunk_data = heapq.heappop(self.heap)
                    chunk_bytes = bytes(chunk_data)

                    async with self.condition:
                        self.condition.notify()

                    if md5_hasher:
                        md5_hasher.update(chunk_bytes)

                    yield chunk_bytes

                    lenght = len(chunk_bytes)
                    next_offset += lenght
                    continue

                # 2. Если куча пуста или там лежат "далекие" куски - ждем новых
                try:
                    #  Пришли данные
                    chunk_start, chunk_data = await self._stream_queue.get()

                except asyncio.CancelledError:
                    break

                # 3. Пришел кусок. Это тот, который мы ждем?
                if chunk_start == next_offset:
                    chunk_bytes = bytes(chunk_data)

                    if md5_hasher:
                        md5_hasher.update(chunk_bytes)

                    yield chunk_bytes

                    lenght = len(chunk_bytes)
                    next_offset += lenght
                else:
                    # Это кусок из будущего. Сохраняем в кучу.
                    # (Внимание: тут может закончиться память, если "первый" кусок завис!)

                    heapq.heappush(self.heap, (chunk_start, chunk_data))
            # ПРОВЕРКА ЦЕЛОСТНОСТИ
            if md5_hasher:
                calculated = md5_hasher.hexdigest()
                if calculated != expected_checksum:
                    err_msg = (
                        f"CRITICAL: Integrity Check Failed!\n"
                        f"Expected: {expected_checksum}\n"
                        f"Got:      {calculated}"
                    )
                    if self._monitor:
                        self._monitor.log(f"[bold red]{err_msg}[/]")

                    # Бросаем исключение. Это прервет consumer-а.
                    raise ValueError(err_msg)

                elif self._monitor:
                    self._monitor.log(f"[green]MD5 Verified: {calculated}[/]")

            if next_offset != total_size:
                raise ValueError(
                    f"Incomplete stream! Got {next_offset} of {total_size}"
                )
        finally:
            del self.files[filename]
            async with self.condition:
                self.condition.notify_all()

    async def run(
        self,
        links: str | Iterable[str],
        expected_checksums: dict[str, str] | None = None,
    ) -> None:
        self._stream = False
        if isinstance(links, str):
            links = [links]
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

        # 1. Запускаем фоновые задачи
        self.task_creator = asyncio.create_task(
            self._add_task_producer(links, expected_checksums)
        )
        self.autosave_task = asyncio.create_task(self._autosave())

        self.workers = [
            asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)
        ]

        try:
            # 2. Ждем Продюсера (пока он переберет все ссылки и набьет очередь)
            await self.task_creator

            # 3. Ждем Воркеров (пока очередь опустеет)
            await self._queue.join()

            if self._monitor and self.is_running:
                self._monitor.log("[green]Все загрузки завершены![/]")

            # Удаление стейтов
            if self.is_running:
                for fname in self.files:
                    self._get_state_path(fname).unlink(missing_ok=True)

            if self.is_running:  # Только если нас не остановили Ctrl+C
                for fname, file in self.files.items():
                    file_path = self._get_file_path(fname)
                    # 1. Проверяем физический размер файла
                    if file_path.is_file():
                        actual_size = file_path.stat().st_size
                        expected_size = file.content_length

                        if expected_size and actual_size != expected_size:
                            err_msg = f"[!] Файл битый: {fname} ({actual_size} != {expected_size})"
                            if self._monitor:
                                self._monitor.log(f"[red]{err_msg}[/]")
                            raise ValueError(err_msg)

                        # Если все ок - красим в зеленый
                        if self._monitor:
                            self._monitor.done(fname)  # Просто визуальный эффект!

        except asyncio.CancelledError:
            pass

        finally:
            self.stop()

            # Гасим всех
            for w in self.workers:
                w.cancel()
            await asyncio.gather(*self.workers, return_exceptions=True)

            # Финальное сохранение
            self._save_all_states()
            await self.client.aclose()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def __aenter__(self) -> Self:
        if self._monitor:
            self._monitor.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:

        if self._monitor:
            self._monitor.stop()
