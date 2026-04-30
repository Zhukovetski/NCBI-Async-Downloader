import asyncio
from collections import defaultdict
from dataclasses import field

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


@my_dataclass
class StateKeeperActor:
    inbox: asyncio.Queue[object | None]
    autosaver_outbox: asyncio.Queue[object]
    _files: dict[int, File] = field(default_factory=dict[int, File])
    _ui_deltas: defaultdict[int, int] = field(default_factory=lambda: defaultdict(int))

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

                case GetUIDeltasCmd(reply_to=queue):
                    await queue.put(dict(self._ui_deltas))
                    self._ui_deltas.clear()

                case _:
                    pass
