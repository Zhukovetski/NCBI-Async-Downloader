import asyncio
import math
import time

from hydrastream.actors.controller import ScaleDownSignal, ScaleUpSignal
from hydrastream.exceptions import LogStatus
from hydrastream.models import UIState, my_dataclass
from hydrastream.monitor import log
from hydrastream.utils import format_size


@my_dataclass
class TelemetryAnalyzer:
    threads: int
    current_limit: int
    analyzer_checkpoint_event: asyncio.Event
    controller_outbox: asyncio.Queue[object]
    smoothed_speed: float = 0.0
    prev_speed: float = 0.0
    tau: float = 1.0
    min_window: int = 1024
    sensitivity: float = 0.05
    is_debug: bool

    ui: UIState

    stop_analyzer: asyncio.Event

    async def run(self) -> None:
        while not self.stop_analyzer.is_set():
            try:
                await self.analyzer_checkpoint_event.wait()

                self.analyzer_checkpoint_event.clear()

                # Просто делаем шаг
                await self._step()

            except Exception as e:
                if self.is_debug:
                    raise
                await log(
                    self.ui,
                    f"Adaptive controller failed: {e}",
                    status=LogStatus.ERROR,
                )

    def _calculate_ema(self, speed_now: float, elapsed: float) -> float:
        if self.smoothed_speed == 0.0:
            return speed_now
        alpha = 1.0 - math.exp(-elapsed / self.tau)
        return (alpha * speed_now) + ((1.0 - alpha) * self.smoothed_speed)

    def _update_window(self, speed_now: float, elapsed: float) -> None:
        safe_speed = max(speed_now, 0.001)
        coef = 1 / safe_speed**0.25
        new_bytes = int(self.bytes_to_check * (1 - coef + elapsed))
        self.bytes_to_check = max(self.min_window, new_bytes)

    async def _log_scale_event(self, direction: str, speed: float) -> None:
        """Вспомогательный метод для чистого логирования."""
        if direction == "up":
            msg = (
                f"Speed increased to {format_size(speed)}/s. "
                f"Scaling up to {self.current_limit} workers."
            )
            status = LogStatus.INFO
            key = "scale_up"
        else:
            msg = f"Network congested. Scaling down to {self.current_limit} workers."
            status = LogStatus.WARNING
            key = "scale_down"

        await log(self.ui, msg, status=status, throttle_key=key, throttle_sec=5.0)

    async def _step(self) -> None:
        """Один шаг адаптации."""
        now = time.monotonic()
        elapsed = min(1, now - self.last_checkpoint_time)
        if elapsed <= 0:
            return

        speed_now = self.bytes_to_check / elapsed
        self.smoothed_speed = self._calculate_ema(speed_now, elapsed)
        self._update_window(speed_now, elapsed)

        # Логика изменения лимита
        if self.smoothed_speed > self.prev_speed * (1 + self.sensitivity):
            if self.current_limit < self.threads:
                self.current_limit += 1
                self.prev_speed = self.smoothed_speed
                await self._log_scale_event("up", speed_now)

        elif (
            self.smoothed_speed < self.prev_speed * (1 - self.sensitivity)
            and self.current_limit > 2
        ):
            self.current_limit -= 1
            self.prev_speed = self.smoothed_speed
            await self._log_scale_event("down", speed_now)

        # Применяем изменения
        if self.dynamic_limit != self.current_limit:
            await self.controller_outbox.put(
                ScaleUpSignal()
                if self.current_limit > self.dynamic_limit
                else ScaleDownSignal()
            )
            self.dynamic_limit = self.current_limit

        self.last_checkpoint_time = time.monotonic()
