import asyncio
import heapq
import os
import signal
from collections.abc import AsyncGenerator
from typing import Self

import httpx
import orjson
from aiolimiter import AsyncLimiter

from .models import Chunk
from .monitor import ProgressMonitor


class NCBILoader:
    def __init__(
        self,
        threads: int = 3,
        silent: bool = False,
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
        # Лимитер запросов (HEAD)
        self.limiter = AsyncLimiter(threads * 2, 1)
        # Семафор соединений (GET)
        self.semaphore = asyncio.Semaphore(threads)
        self._timeout = httpx.Timeout(timeout, read=5.0)
        self.client = httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=follow_redirects,
            verify=verify,
            http2=http2,
        )

        # ! ВАЖНО: Очередь ограничивает аппетит add_task
        # Умножаем на 2, чтобы был небольшой буфер, но не слишком большой
        self.queue = asyncio.Queue(maxsize=threads * 2)
        self._stream_queue = asyncio.Queue(
            maxsize=(
                self.max_conns * 2
                if not stream_buffer_size
                else stream_buffer_size // self.MIN_CHUNK
            )
        )

        self.file_states = {}
        self.stop_add_task = asyncio.Event()
        self.stop_add_task.set()
        self.is_running = True
        self.workers = None
        self.producer_task = None
        self.task_creator = None
        self.autosave_task = None

    def _get_state_path(self, filename: str) -> str:
        return f"{filename}.state.json"

    def save_all_states(self) -> None:
        if self.monitor:
            self.monitor.log("[yellow]Сохранение состояния...[/]")

        # Копируем ключи, чтобы не словить ошибку изменения словаря во время итерации
        # (хотя в однопоточном asyncio это редкость, но береженого бог бережет)
        for filename in list(self.file_states.keys()):
            chunks = self.file_states[filename]
            # Сохраняем, только если файл не завершен
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
        # Простейшая защита от записи пустоты
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

    async def add_task_producer(self, links: list[str]) -> None:
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
                        with open(filename, "wb") as f:
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

                if self.monitor:
                    # Важно: добавляем total_size
                    # Вычисляем его из чанков (на случай восстановления)
                    total_calc = chunks[-1].end + 1
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
                        fd = os.open(chunk.filename, os.O_RDWR)
                        BUFFER_SIZE = 1024 * 1024  # 1 MB
                        try:
                            async for data in r.aiter_bytes():
                                if not self.is_running:
                                    break

                                downloaded_in_this_attempt += len(data)
                                buffer.extend(data)
                                if self.monitor:
                                    self.monitor.update(chunk.filename, len(data))
                                if len(buffer) >= BUFFER_SIZE:
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
        # Нам НЕ НАДО ждать их здесь через gather! Мы их просто уведомляем.
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

    async def stream(self, links: str) -> AsyncGenerator[bytes, None]:
        self._stream = True
        self.producer_task = asyncio.create_task(self.run(links))
        next_offset = 0
        heap = []  # Куча для хранения чанков, пришедших раньше времени
        try:
            while self.is_running:
                # 1. Если в куче уже есть следующий кусок - отдаем его
                if heap and heap[0][0] == next_offset:
                    _, chunk_data = heapq.heappop(heap)
                    yield bytes(chunk_data)
                    next_offset += len(chunk_data)
                    continue

                # 2. Если куча пуста или там лежат "далекие" куски - ждем новых
                try:
                    # Ждем чанк от воркера
                    # Создаем задачу на получение из очереди
                    get_task = asyncio.create_task(self._stream_queue.get())

                    # Ждем, кто победит: данные или конец работы продюсера
                    done, pending = await asyncio.wait(
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
                    next_offset += len(chunk_data)
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

        finally:
            if not self.producer_task.done():
                self.producer_task.cancel()
            await asyncio.gather(self.producer_task, return_exceptions=True)

    async def run(self, links: str | list[str]) -> None:
        if self.producer_task is None:
            self._stream = False
        if isinstance(links, str):
            links = [links]
        # Хендлеры сигналов лучше ставить в main(), но можно и тут
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

        except asyncio.CancelledError:
            pass

        finally:
            self.stop()  # Флаг вниз

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

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.monitor:
            self.monitor.stop()
