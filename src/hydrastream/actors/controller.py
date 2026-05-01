# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio

from hydrastream.models import my_dataclass


@my_dataclass(frozen=True)
class NetworkCongestionSignal:
    pass


@my_dataclass(frozen=True)
class MaxLimitSignal:
    pass


@my_dataclass(frozen=True)
class ScaleUpSignal:
    pass


@my_dataclass(frozen=True)
class ScaleDownSignal:
    pass


@my_dataclass
class TrafficController:
    reg_events_q: asyncio.Queue[object]
    worker_events: list[asyncio.Event]
    stop_analyzer: asyncio.Event
    controller_checkpoint_event: asyncio.Event

    dynamic_limit: int = 1
    prev_dynamic_limit: int = 1

    def __post_init__(self) -> None:
        for i in range(self.dynamic_limit):
            self.worker_events[i].set()

    async def run(self) -> None:
        while True:
            msg = await self.reg_events_q.get()

            if isinstance(msg, NetworkCongestionSignal | ScaleDownSignal):
                self.dynamic_limit = max(1, self.dynamic_limit - 1)

            elif isinstance(msg, ScaleUpSignal):
                self.dynamic_limit = min(
                    len(self.worker_events), self.dynamic_limit + 1
                )

            elif isinstance(msg, MaxLimitSignal):
                self.dynamic_limit = len(self.worker_events)
                self._update_lights()
                self.stop_analyzer.set()
                self.controller_checkpoint_event.set()
                break

            if self.dynamic_limit != self.prev_dynamic_limit:
                self._update_lights()

    def _update_lights(self) -> None:
        start, end = sorted((self.prev_dynamic_limit, self.dynamic_limit))
        events_to_update = self.worker_events[start:end]

        if self.dynamic_limit > self.prev_dynamic_limit:
            for event in events_to_update:
                event.set()
        else:
            for event in events_to_update:
                event.clear()

        self.prev_dynamic_limit = self.dynamic_limit
