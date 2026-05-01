import asyncio
from collections import defaultdict
from dataclasses import field
from typing import TypeAlias

from hydrastream.models import File, my_dataclass


@my_dataclass(frozen=True)
class RegisterFileCmd:
    file_id: int
    file_obj: File


@my_dataclass(frozen=True)
class RemoveFileCmd:
    file_id: int


@my_dataclass(frozen=True)
class GetSnapshotCmd:
    reply_to: asyncio.Queue[dict[int, File]]


@my_dataclass(frozen=True)
class ProgressDeltaCmd:
    file_id: int
    delta_bytes: int


@my_dataclass(frozen=True)
class GetUIDeltasCmd:
    reply_to: asyncio.Queue[dict[int, int]]


StateKeeperCmd: TypeAlias = (
    RegisterFileCmd
    | RemoveFileCmd
    | GetSnapshotCmd
    | ProgressDeltaCmd
    | GetUIDeltasCmd
    | None
)


@my_dataclass
class StateKeeperActor:
    inbox: asyncio.Queue[StateKeeperCmd]

    _files: dict[int, File] = field(default_factory=dict[int, File])
    _ui_deltas: defaultdict[int, int] = field(default_factory=lambda: defaultdict(int))

    _global_bytes: int = 0
    _prev_global_bytes: int = 0

    bytes_to_check: int

    analyzer_checkpoint_event: asyncio.Event
    throttler_checkpoint_event: asyncio.Event

    async def run(self) -> None:
        while True:
            cmd = await self.inbox.get()
            match cmd:
                case None:
                    break

                case RegisterFileCmd(file_id=fid, file_obj=fobj):
                    self._files[fid] = fobj

                case RemoveFileCmd(file_id=fid):
                    self._files.pop(fid, None)

                case GetSnapshotCmd(reply_to=queue):
                    await queue.put(self._files.copy())

                case ProgressDeltaCmd(file_id=fid, delta_bytes=delta):
                    self._ui_deltas[fid] += delta
                    self._global_bytes += delta

                    if (
                        self._global_bytes - self._prev_global_bytes
                        >= self.bytes_to_check
                    ):
                        self._prev_global_bytes += self.bytes_to_check

                        self.analyzer_checkpoint_event.set()
                        self.throttler_checkpoint_event.set()

                case GetUIDeltasCmd(reply_to=queue):
                    await queue.put(dict(self._ui_deltas))
                    self._ui_deltas.clear()

                case _:
                    pass
