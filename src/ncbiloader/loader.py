# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import heapq
import os
import signal
from collections.abc import AsyncGenerator
from pathlib import Path
from types import TracebackType
from typing import Iterable, Self

import httpx
import orjson
from aiolimiter import AsyncLimiter

from .models import Chunk
from .monitor import ProgressMonitor


class NCBILoader:
    queue: asyncio.Queue[Chunk]
    _stream_queue: asyncio.Queue[tuple[int, bytearray]]
    workers: list[asyncio.Task[None]] = []

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

        self.max_conns = threads
        self.monitor = None
        self._stream = None
        self.stream_buffer_size = stream_buffer_size
        self.MIN_CHUNK = 5 * 1024 * 1024  # 5 MB минимум
        if not silent:
            self.monitor = ProgressMonitor()
        self.limiter = AsyncLimiter(threads * 2, 1)
        self.semaphore = asyncio.Semaphore(threads)
        self._timeout = httpx.Timeout(timeout, read=5.0)
        self.client = httpx.AsyncClient(
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

        self.queue = asyncio.Queue(maxsize=threads * 2)
        self._stream_queue = asyncio.Queue(
            maxsize=(
                self.max_conns * 2
                if not stream_buffer_size
                else stream_buffer_size // self.MIN_CHUNK
            )
        )
        self._get_expected_size: dict[str, int] = {}
        self.file_states: dict[str, list[Chunk]] = {}
        self.stop_add_task = asyncio.Event()
        self.stop_add_task.set()
        self.is_running = True
        self.workers = []
        self.producer_task = None
        self.task_creator = None
        self.autosave_task = None

    def _get_state_path(self, filename: str) -> Path:
        return self.state_dir / f"{filename}.state.json"

    def _get_file_path(self, filename: str) -> Path:
        return self.output_dir / filename

    def save_all_states(self) -> None:
        if self.monitor:
            self.monitor.log("[yellow]Сохранение состояния...[/]")

        for filename in list(self.file_states.keys()):
            chunks = self.file_states[filename]
            if not all(c.current_pos > c.end for c in chunks):
                self._save_state(filename)

    async def autosave(self) -> None:
        while self.is_running:
            try:
                await asyncio.sleep(60)
                self.save_all_states()
            except asyncio.CancelledError:
                break

    def _save_state(self, filename: str) -> None:
        if filename not in self.file_states:
            return

        try:
            # Сериализуем список dataclass'ов
            data = [c for c in self.file_states[filename]]
            with open(self._get_state_path(filename), "wb") as f:
                f.write(orjson.dumps(data))
        except Exception as e:
            if self.monitor:
                self.monitor.log(f"[red]Ошибка сохранения {filename}: {e}[/]")

    async def add_task_producer(self, links: Iterable[str]) -> None:
        """Продюсер: Нарезает файлы и кидает в очередь"""
        for url in links:
            if not self.is_running:
                break

            try:
                filename = url.rsplit("/", 1)[1]
                chunks = []
                state_path = self._get_state_path(filename)

                # 1. Попытка восстановления
                if not self._stream:
                    if os.path.exists(state_path):
                        try:
                            with open(state_path, "rb") as f:
                                content = f.read()
                                if content:
                                    chunks = [Chunk(**c) for c in orjson.loads(content)]
                        except Exception:
                            if self.monitor:
                                self.monitor.log(f"[!] Битая история {filename}")
                            chunks = []

                    elif os.path.exists(filename):
                        if self.monitor:
                            self.monitor.log(f"[red]{filename} уже существует![/]")
                        continue

                # 2. Если не восстановили - создаем с нуля
                if not chunks:
                    async with self.limiter:
                        resp = await self.client.head(url)
                        if resp.status_code >= 400:
                            if self.monitor:
                                self.monitor.log(
                                    f"[red]Ошибка {resp.status_code} для {filename}[/]"
                                )
                            continue  # Пропускаем этот файл
                        total_size = int(resp.headers.get("Content-Length", 0))

                    # Sparse file creation
                    if not self._stream:
                        if self.monitor:
                            self.monitor.log(f"[*] Новый: {filename}")
                        with open(self._get_file_path(filename), "wb") as f:
                            f.truncate(total_size)

                    # Логика нарезки
                    parts = self.max_conns
                    chunk_size = (
                        max(total_size // parts, self.MIN_CHUNK)
                        if not self._stream
                        else self.MIN_CHUNK
                    )
                    # Пересчитываем parts, если файл маленький
                    parts = (total_size + chunk_size - 1) // chunk_size

                    for i in range(parts):
                        start = i * chunk_size
                        end = min((i + 1) * chunk_size - 1, total_size - 1)
                        chunks.append(
                            Chunk(
                                url=url,
                                start=start,
                                end=end,
                                current_pos=start,
                                filename=filename,
                            )
                        )

                # 3. Регистрируем в UI и памяти
                self.file_states[filename] = chunks
                total_calc = chunks[-1].end + 1
                self._get_expected_size[filename] = total_calc
                if self.monitor:
                    # Важно: добавляем total_size
                    # Вычисляем его из чанков (на случай восстановления)

                    self.monitor.add_file(filename, total_calc)

                    downloaded = sum(c.current_pos - c.start for c in chunks)
                    if downloaded > 0:
                        self.monitor.update(filename, downloaded)

                # 4. Кидаем в очередь (самое важное)
                for c in chunks:
                    if not self.is_running:
                        break
                    await self.stop_add_task.wait()
                    if c.current_pos <= c.end:
                        await self.queue.put(c)

            except Exception as e:
                if self.monitor:
                    self.monitor.log(f"[red]Ошибка обработки {url}: {e}[/]")

    async def download_worker(self) -> None:
        while self.is_running:
            chunk = await self.queue.get()
            try:
                await self._process_chunk(chunk)
            except asyncio.CancelledError:
                break
            except Exception:
                if self.is_running:
                    # Мягкий ретрай
                    await asyncio.sleep(2)
                    await self.queue.put(chunk)
            finally:
                self.queue.task_done()

    async def _process_chunk(self, chunk: Chunk) -> None:
        async with self.semaphore:
            downloaded_in_this_attempt = 0
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
            try:
                async with self.client.stream("GET", chunk.url, headers=headers) as r:
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
                                if self.monitor:
                                    self.monitor.update(chunk.filename, len(data))
                                if len(buffer) >= buffer_size:
                                    await self._write_buffer(fd, buffer, chunk)
                                    buffer = bytearray()
                            if buffer:
                                await self._write_buffer(fd, buffer, chunk)
                        finally:
                            os.close(fd)
                    else:
                        async for data in r.aiter_bytes():
                            if not self.is_running:
                                break
                            downloaded_in_this_attempt += len(data)
                            buffer.extend(data)
                            if self.monitor:
                                self.monitor.update(chunk.filename, len(data))
                        await self._stream_queue.put((chunk.start, buffer))
            except Exception:
                if self.monitor and downloaded_in_this_attempt:
                    self.monitor.update(chunk.filename, -downloaded_in_this_attempt)
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

        if self.monitor:
            self.monitor.log("[yellow]Получен сигнал остановки...[/]")

        # 1. Останавливаем Продюсеров (чтобы не кидали новое)
        if self.task_creator:
            self.task_creator.cancel()
        if self.autosave_task:
            self.autosave_task.cancel()
        if self.producer_task:
            self.producer_task.cancel()

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
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
        # Всё. Мы почистили очередь и пнули воркеров.
        # Теперь queue.join() в методе run() разблокируется (или почти сразу).
        # И управление перейдет в блок finally метода run().

    async def stream_all(
        self, links: Iterable[str], expected_checksums: dict[str, str] | None = None
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes, None]], None]:
        """
        Принимает итератор ссылок.
        Отдает последовательно пары: (имя_файла, генератор_байт_этого_файла).
        """
        # Если передан словарь с хешами {url: md5}, будем его использовать
        if expected_checksums is None:
            expected_checksums = {}

        for url in links:
            # 1. Определяем имя (можно вынести в утилиту)
            filename = url.rsplit("/", 1)[1]

            # 2. Ищем ожидаемый хеш для этого файла
            md5 = expected_checksums.get(url)

            # 3. Создаем генератор для ЭТОГО файла
            file_gen = self.stream(url, expected_checksum=md5)

            yield filename, file_gen

    async def stream(self, links: str | Iterable[str]) -> AsyncGenerator[bytes, None]:
        self._stream = True
        self.producer_task = asyncio.create_task(self.run(links))
        bytes_yielded = 0
        next_offset = 0
        heap: list[
            tuple[int, bytearray]
        ] = []  # Куча для хранения чанков, пришедших раньше времени
        try:
            while self.is_running:
                # 1. Если в куче уже есть следующий кусок - отдаем его
                if heap and heap[0][0] == next_offset:
                    _, chunk_data = heapq.heappop(heap)
                    yield bytes(chunk_data)
                    lenght = len(chunk_data)
                    bytes_yielded += lenght
                    next_offset += lenght
                    continue

                # 2. Если куча пуста или там лежат "далекие" куски - ждем новых
                try:
                    # Ждем чанк от воркера
                    # Создаем задачу на получение из очереди
                    get_task = asyncio.create_task(self._stream_queue.get())

                    # Ждем, кто победит: данные или конец работы продюсера
                    done, _pending = await asyncio.wait(
                        [get_task, self.producer_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # Сценарий А: Продюсер умер (или закончил штатно)
                    if self.producer_task in done:
                        # Если продюсер всё, но в очереди еще что-то есть - забираем
                        if get_task.done():
                            chunk_start, chunk_data = get_task.result()
                        else:
                            # Продюсер мертв, и данных нет. Отменяем ожидание и выходим.
                            get_task.cancel()
                            break

                    # Сценарий Б: Пришли данные
                    else:
                        chunk_start, chunk_data = get_task.result()
                except asyncio.CancelledError:
                    break

                # 3. Пришел кусок. Это тот, который мы ждем?
                if chunk_start == next_offset:
                    yield bytes(chunk_data)
                    lenght = len(chunk_data)
                    bytes_yielded += lenght
                    next_offset += lenght
                else:
                    # Это кусок из будущего. Сохраняем в кучу.
                    # (Внимание: тут может закончиться память, если "первый" кусок завис!)
                    heapq.heappush(heap, (chunk_start, chunk_data))
                    if len(heap) > 20:
                        self.stop_add_task.clear()
                    else:
                        self.stop_add_task.set()
                # Если закачка завершилась и мы все отдали
                if (
                    self.producer_task.done()
                    and not heap
                    and self._stream_queue.empty()
                ):
                    break
            if bytes_yielded != total_size:
                raise ValueError(
                    f"Incomplete stream! Got {bytes_yielded} of {total_size}"
                )

        finally:
            if not self.producer_task.done():
                self.producer_task.cancel()
            await asyncio.gather(self.producer_task, return_exceptions=True)

    async def run(self, links: str | Iterable[str]) -> None:
        if self.producer_task is None:
            self._stream = False
        if isinstance(links, str):
            links = [links]
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

        # 1. Запускаем фоновые задачи
        self.task_creator = asyncio.create_task(self.add_task_producer(links))
        if not self._stream:
            self.autosave_task = asyncio.create_task(self.autosave())

        self.workers = [
            asyncio.create_task(self.download_worker()) for _ in range(self.max_conns)
        ]

        try:
            # 2. Ждем Продюсера (пока он переберет все ссылки и набьет очередь)
            await self.task_creator

            # 3. Ждем Воркеров (пока очередь опустеет)
            await self.queue.join()
            await self._stream_queue.join()

            if self.monitor and self.is_running:
                self.monitor.log("[green]Все загрузки завершены![/]")

            # Удаление стейтов
            if self.is_running and not self._stream:
                for url in links:
                    fname = url.rsplit("/", 1)[1]
                    spath = self._get_state_path(fname)
                    if os.path.exists(spath):
                        os.remove(spath)
            if self.is_running:  # Только если нас не остановили Ctrl+C
                for url in links:  # Или пройтись по self.file_states
                    filename = url.rsplit("/", 1)[1]

                    # 1. Проверяем физический размер файла
                    if os.path.exists(filename):
                        actual_size = os.path.getsize(filename)
                        # Достаем ожидаемый размер из стейта или памяти
                        # (Тебе нужно сохранить total_size где-то в self.file_meta[filename] при старте)
                        expected_size = self._get_expected_size[filename]

                        if expected_size and actual_size != expected_size:
                            err_msg = f"[!] Файл битый: {filename} ({actual_size} != {expected_size})"
                            if self.monitor:
                                self.monitor.log(f"[red]{err_msg}[/]")
                            raise ValueError(err_msg)

                        # Если все ок - красим в зеленый
                        if self.monitor:
                            self.monitor.done(filename)  # Просто визуальный эффект!
                            self.monitor.log(f"[green]OK: {filename}[/]")

        except asyncio.CancelledError:
            pass

        finally:
            self.stop()

            # Гасим всех
            for w in self.workers:
                w.cancel()
            await asyncio.gather(*self.workers, return_exceptions=True)

            # Финальное сохранение
            if not self._stream:
                self.save_all_states()
            await self.client.aclose()

    async def __aenter__(self) -> Self:
        if self.monitor:
            self.monitor.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:

        if self.monitor:
            self.monitor.stop()
