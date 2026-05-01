# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import random
from abc import ABC, abstractmethod

from curl_cffi import Headers, Response
from curl_cffi.requests import RequestsError

from hydrastream._curl_shim import get_error_response
from hydrastream.actors.stater import ProgressDeltaCmd, RegisterFileCmd, StateKeeperCmd
from hydrastream.engine import send_poison_pills
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.models import (
    Checksum,
    Envelope,
    File,
    FileMeta,
    LinkData,
    NetworkState,
    TypeHash,
    UIState,
    my_dataclass,
)
from hydrastream.monitor import add_file, done, log
from hydrastream.network import extract_filename, safe_request, stream_chunk
from hydrastream.providers import ProviderRouter
from hydrastream.utils import redact_url


@my_dataclass
class BaseMetadataResolver(ABC):
    threads: int
    MIN_CHUNK: int

    links_inbox: asyncio.PriorityQueue[Envelope[LinkData | None]]
    files_outbox: asyncio.PriorityQueue[Envelope[File | None]]
    reg_events_outbox: asyncio.Queue[object]

    num_dispathers: int
    all_complete: asyncio.Event

    is_dry_run: bool
    is_verify: bool

    ui: UIState
    net: NetworkState

    async def run(self) -> None:
        """Это ШАБЛОННЫЙ МЕТОД. Наследники не переопределяют его!"""
        checksum = None
        while True:
            envelope = await self.links_inbox.get()

            if envelope.is_poison_pill:
                if envelope.is_last_survivor:
                    await send_poison_pills(self.files_outbox, self.num_dispathers)
                    if self.is_dry_run:
                        self.all_complete.set()
                break

            if not (data := envelope.payload):
                continue

            try:
                meta = await self._fetch_metadata(data.url)
                filename, total_size, supports_ranges = meta

                if self.is_verify and not data.checksum:
                    checksum = await self._resolve_hash(
                        data.id, data.url, filename, data.checksum
                    )

                # ВЫЗЫВАЕМ АБСТРАКТНЫЙ МЕТОД (Делегируем создание наследнику)
                file_obj = await self._prepare_file_object(
                    data=data,
                    filename=filename,
                    total_size=total_size,
                    supports_ranges=supports_ranges,
                    checksum=checksum,
                )

                await self._register_file(file_obj)

            except Exception as e:
                await self._handle_error(e, envelope, data)

    async def _register_file(self, file_obj: File) -> None:
        """Общая логика регистрации, внутри которой есть ХУК для наследников."""
        filename = file_obj.meta.original_filename

        await self.reg_events_outbox.put(
            RegisterFileCmd(file_id=file_obj.meta.id, file_obj=file_obj)
        )
        add_file(self.ui, file_obj.meta.id, filename, file_obj.meta.content_length)

        # ВЫЗЫВАЕМ ХУК (Стрим проигнорирует, Диск - обновит UI)
        await self._on_file_registered(file_obj)

        await self.files_outbox.put(
            Envelope(sort_key=(file_obj.meta.id,), payload=file_obj)
        )

    @abstractmethod
    async def _prepare_file_object(
        self,
        data: LinkData,
        filename: str,
        total_size: int,
        supports_ranges: bool,
        checksum: Checksum | None,
    ) -> File:
        pass

    @abstractmethod
    async def _on_file_registered(self, file_obj: File) -> None:
        pass

    async def _handle_error(
        self,
        e: Exception,
        envelope: Envelope[LinkData | None],
        data: LinkData,
    ) -> None:
        """Возвращает True, если нужно пропустить итерацию (continue)."""

        if isinstance(e, RequestsError):
            response = get_error_response(e)

            if isinstance(response, Response):
                status = response.status_code
                # Постоянные ошибки: логируем и забываем
                if status in {400, 401, 403, 404, 410, 416}:
                    await log(
                        self.ui,
                        f"Link {redact_url(data.url)} failed permanently "
                        f"(HTTP {status}).",
                        status=LogStatus.ERROR,
                    )

                # Временные ошибки сервера (5xx, 429) — в очередь
                await self._requeue_chunk(envelope, delay_range=(0.5, 2.0))
            else:
                # Сетевая ошибка без ответа
                await self._requeue_chunk(envelope)

        if isinstance(e, TimeoutError):
            await self._requeue_chunk(envelope)

        # Если мы здесь, значит ошибка критическая (Exception)
        await log(
            self.ui, f"Critical Task Creator crash: {e!r}", status=LogStatus.CRITICAL
        )
        raise e

    async def _requeue_chunk(
        self,
        envelope: Envelope[LinkData | None] | None,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        if envelope is None:
            return
        await self.links_inbox.put(envelope)
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def _fetch_metadata(self, url: str) -> tuple[str, int, bool]:
        # 1. Пробуем HEAD
        response = await safe_request(self.net, "HEAD", url=url)
        # 2. Если HEAD не дал инфы, используем GET, но ОБЯЗАТЕЛЬНО через stream
        if response is None or int(response.headers.get("content-length", 0)) == 0:
            # Контекстный менеджер 'async with' сам закроет соединение в конце
            async with stream_chunk(self.net, url) as resp:
                headers = resp.headers
                return self._parse_headers(url, headers)

        return self._parse_headers(url, response.headers)

    def _parse_headers(self, url: str, headers: Headers) -> tuple[str, int, bool]:
        total_size = int(headers.get("content-length", 0))

        accept_ranges = headers.get("accept-ranges", "").lower()
        supports_ranges = (accept_ranges == "bytes") and (total_size > 0)
        filename = extract_filename(url, headers)
        return filename, total_size, supports_ranges

    async def _resolve_hash(
        self,
        id: int,
        url: str,
        filename: str,
        checksum_tuple: tuple[TypeHash, str] | None,
    ) -> Checksum | None:
        if checksum_tuple:
            return Checksum(algorithm=checksum_tuple[0], value=checksum_tuple[1])

        add_file(self.ui, id, filename)

        provider = ProviderRouter()
        checksum = await provider.resolve_hash(self.net, url, filename)
        await done(self.ui, id, filename)

        if checksum is None:
            await log(
                self.ui,
                f"Missing MD5 hash for file: {filename}",
                status=LogStatus.WARNING,
            )

        return checksum


@my_dataclass
class StreamMetadataResolver(BaseMetadataResolver):
    # Специфичная зависимость только для стрима!
    STREAM_CHUNK_SIZE: int

    async def _prepare_file_object(
        self,
        data: LinkData,
        filename: str,
        total_size: int,
        supports_ranges: bool,
        checksum: Checksum | None,
    ) -> File:
        chunk_size = (
            max(total_size // self.threads, self.MIN_CHUNK) if total_size > 0 else 0
        )
        chunk_size = min(chunk_size, self.STREAM_CHUNK_SIZE)

        return File(
            meta=FileMeta(
                id=data.id,
                original_filename=filename,
                url=data.url,
                content_length=total_size,
                supports_ranges=supports_ranges,
                expected_checksum=checksum,
            ),
            chunk_size=chunk_size,
        )

    async def _on_file_registered(self, file_obj: File) -> None:
        # В режиме стрима нам не нужно пересчитывать скачанные байты для UI!
        pass


@my_dataclass
class DiskMetadataResolver(BaseMetadataResolver):
    state_outbox: asyncio.Queue[StateKeeperCmd]

    fs: StorageBackend

    async def _prepare_file_object(
        self,
        data: LinkData,
        filename: str,
        total_size: int,
        supports_ranges: bool,
        checksum: Checksum | None,
    ) -> File:
        chunk_size = (
            max(total_size // self.threads, self.MIN_CHUNK) if total_size > 0 else 0
        )

        file_obj = None
        if supports_ranges:
            file_obj, num_states = self.fs.load_state(filename=filename)
            if num_states > 1:
                await log(
                    self.ui,
                    f"Multiple state files found for {filename}!",
                    status=LogStatus.WARNING,
                )

        if file_obj:
            return file_obj

        return File(
            meta=FileMeta(
                id=data.id,
                original_filename=filename,
                url=data.url,
                content_length=total_size,
                supports_ranges=supports_ranges,
                expected_checksum=checksum,
            ),
            chunk_size=chunk_size,
        )

    async def _register_file(self, file_obj: File) -> None:
        filename = file_obj.meta.original_filename
        await self.reg_events_outbox.put(
            RegisterFileCmd(file_id=file_obj.meta.id, file_obj=file_obj)
        )
        chunks = file_obj.chunks or []

        add_file(self.ui, file_obj.meta.id, filename, file_obj.meta.content_length)
        downloaded = sum(c.uploaded for c in chunks)
        if downloaded - len(chunks) > 0:
            await self.state_outbox.put(
                ProgressDeltaCmd(file_id=file_obj.meta.id, delta_bytes=downloaded)
            )
        await self.files_outbox.put(
            Envelope(sort_key=(file_obj.meta.id,), payload=file_obj)
        )
