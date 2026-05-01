# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import time
from typing import TypeAlias

from curl_cffi import CurlOpt, Response

from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.models import UIState, my_dataclass
from hydrastream.monitor import log


@my_dataclass(frozen=True)
class CheckpointReachedCmd:
    pass


@my_dataclass(frozen=True)
class RegisterStreamCmd:
    stream: Response


@my_dataclass(frozen=True)
class RemoveStreamCmd:
    stream: Response


@my_dataclass(frozen=True)
class DiskBufferFullSignal:
    pass


@my_dataclass(frozen=True)
class DiskBufferClearedSignal:
    pass


ThrottlerMsg: TypeAlias = (
    RegisterStreamCmd
    | RemoveStreamCmd
    | DiskBufferFullSignal
    | DiskBufferClearedSignal
    | CheckpointReachedCmd
    | None
)


@my_dataclass
class ThrottleController:
    active_stream: set[Response]

    speed_limit: float | None
    frequency_speed_limit: int = 10
    time_speed_limit: float
    bytes_to_check: int
    prev_bytes: int = 0
    last_checkpoint_time: float = 0.0
    target_time: float = 0.0

    is_debug: bool
    ui: UIState

    all_complete: asyncio.Event
    throttler_checkpoint_event: asyncio.Event

    throttler_input: asyncio.Queue[ThrottlerMsg]
    is_disk_choked: bool = False

    def __post_init__(self) -> None:
        self.time_speed_limit = 1 / self.frequency_speed_limit
        if self.speed_limit:
            self.speed_limit = self.speed_limit * 1024**2
            self.bytes_to_check = int(self.speed_limit / self.frequency_speed_limit)
            self.target_time = self.bytes_to_check / self.speed_limit
        else:
            self.bytes_to_check = 5 * 1024**2

    async def run(self) -> None:  # noqa
        self.last_checkpoint_time = time.monotonic()

        while not self.all_complete.is_set():
            try:
                # Актор слушает ТОЛЬКО одну очередь!
                msg = await self.throttler_input.get()

                match msg:
                    case None:
                        break

                    case RegisterStreamCmd(stream=s):
                        self.active_stream.add(s)
                        # Если диск УЖЕ тупит, сразу режем скорость новичку!
                        if self.is_disk_choked and s.curl:
                            s.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 1)

                    case RemoveStreamCmd(stream=s):
                        self.active_stream.discard(s)

                    case DiskBufferFullSignal():
                        # Авария на диске! Режем скорость ВСЕМ.
                        self.is_disk_choked = True
                        self._set_curl_speed_limit(limit=1)

                    case DiskBufferClearedSignal():
                        # Диск разгреб завалы!
                        self.is_disk_choked = False
                        self._set_curl_speed_limit(limit=0)

                    case CheckpointReachedCmd():
                        # Пришла порция байтов для ограничения скорости юзера
                        await self.enforce_throttling()

            except Exception as e:
                if self.is_debug:
                    raise
                await log(
                    self.ui, f"Throttle controller failed: {e}", status=LogStatus.ERROR
                )

    async def enforce_throttling(self) -> None:
        now = time.monotonic()
        elapsed = min(1, now - self.last_checkpoint_time)

        if elapsed <= 0 or not self.speed_limit:
            return

        target_time = self.bytes_to_check / self.speed_limit

        if elapsed < target_time:
            sleep_duration = target_time - elapsed

            # Ставим на паузу
            self._set_curl_speed_limit(limit=1)

            await asyncio.sleep(sleep_duration)

            # СНИМАЕМ ПАУЗУ ТОЛЬКО ЕСЛИ ДИСК СВОБОДЕН!
            if not self.is_disk_choked:
                self._set_curl_speed_limit(limit=0)

        # Обновляем время после паузы!
        self.last_checkpoint_time = time.monotonic()

    def _set_curl_speed_limit(self, limit: int) -> None:
        """Вспомогательная функция для прохода по активным потокам."""
        for r in self.active_stream:
            if r.curl is not None:
                r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, limit)
