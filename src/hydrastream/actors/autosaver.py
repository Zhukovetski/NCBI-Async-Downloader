# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio

from hydrastream.actors.aggregator import FlushCmd
from hydrastream.actors.stater import GetSnapshotCmd, StateKeeperCmd
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.models import (
    File,
    StopMsg,
    UIState,
    WriteChunk,
    my_dataclass,
)
from hydrastream.monitor import log


@my_dataclass
class FileAutosaver:
    all_complete: asyncio.Event
    flush_event: asyncio.Event
    disk_q: asyncio.Queue[WriteChunk | FlushCmd | StopMsg]
    reg_events_q: asyncio.Queue[StateKeeperCmd]
    get_shapshot: asyncio.Queue[dict[int, File]]
    fs: StorageBackend
    ui: UIState

    is_debug: bool

    async def run(self, interval: float) -> None:
        loop = asyncio.get_running_loop()

        while not self.all_complete.is_set():
            try:
                async with asyncio.timeout(interval):
                    await self.all_complete.wait()
                break
            except TimeoutError:
                try:
                    self.flush_event.clear()
                    await self.disk_q.put(FlushCmd())
                    await self.reg_events_q.put(
                        GetSnapshotCmd(reply_to=self.get_shapshot)
                    )
                    files = await self.get_shapshot.get()
                    await self.flush_event.wait()
                    await loop.run_in_executor(None, self.save_all_states, files)

                except Exception as e:
                    if self.is_debug:
                        raise
                    await log(
                        self.ui,
                        f"Auto-save operation failed: {e}",
                        status=LogStatus.ERROR,
                    )

    def save_all_states(self, files: dict[int, File]) -> None:
        for file in list(files.values()):
            if file.chunks and not all(
                c.current_pos > c.end for c in (file.chunks or [])
            ):
                self.fs.save_state(file)
