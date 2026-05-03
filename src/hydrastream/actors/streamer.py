import asyncio
import hashlib
from collections.abc import AsyncGenerator

from hydrastream.actors.dispatcher import FileCompleted
from hydrastream.actors.stater import RemoveFileCmd, StateKeeperCmd
from hydrastream.exceptions import FileSizeMismatchError, HashMismatchError, LogStatus
from hydrastream.interfaces import Hasher
from hydrastream.models import (
    Checksum,
    Envelope,
    File,
    StopMsg,
    StreamChunk,
    UIState,
)
from hydrastream.monitor import done, log


async def file_streamer(  # noqa
    file_obj: File,
    stream_chunk_inbox: asyncio.PriorityQueue[Envelope[StreamChunk | StopMsg]],
    credit_outbox: asyncio.Queue[int],
    reg_events_q: asyncio.Queue[StateKeeperCmd],
    file_limit_q: asyncio.Queue[FileCompleted],
    ui: UIState,
    is_debug: bool,
) -> AsyncGenerator[bytes, None]:

    total_size = file_obj.meta.content_length
    checksum = file_obj.meta.expected_checksum
    hasher: Hasher | None = hashlib.new(checksum.algorithm) if checksum else None

    buffer: dict[int, list[bytes]] = {}
    expected_offset = 0

    await log(ui, f"Streaming: {file_obj.actual_filename}", status=LogStatus.INFO)

    try:
        while expected_offset < total_size:
            envelope = await stream_chunk_inbox.get()
            msg = envelope.payload

            match msg:
                case StreamChunk(start=offset, data=chunk_data):
                    if offset == expected_offset:
                        for data in chunk_data:
                            if hasher:
                                hasher.update(data)

                            yield data
                            expected_offset += len(data)
                            await credit_outbox.put(len(data))

                            while expected_offset in buffer:
                                next_data = buffer.pop(expected_offset)

                                for n_data in next_data:
                                    if hasher:
                                        hasher.update(n_data)

                                    yield n_data
                                    expected_offset += len(n_data)
                                    await credit_outbox.put(len(n_data))
                    else:
                        buffer[offset] = chunk_data

                case StopMsg():
                    break

                case _:
                    if is_debug:
                        raise RuntimeError(
                            f"Unknown message type in stream_chunk_inbox: {type(msg)}"
                        )
                    await log(
                        ui,
                        f"Received unknown message: {msg}",
                        status=LogStatus.ERROR,
                    )

        else:
            if hasher and checksum:
                try:
                    verify_stream(
                        hasher,
                        file_obj.actual_filename,
                        checksum,
                        expected_offset,
                        total_size,
                    )
                    await log(
                        ui, "Hash Verified", status=LogStatus.SUCCESS, progress=True
                    )
                except Exception as e:
                    await log(ui, str(e), status=LogStatus.ERROR)
                    raise

            await done(ui, file_obj.meta.id, file_obj.actual_filename)

    finally:
        buffer.clear()

        await reg_events_q.put(RemoveFileCmd(file_id=file_obj.meta.id))
        await file_limit_q.put(FileCompleted())


def verify_stream(
    hasher: Hasher,
    filename: str,
    expected_checksum: Checksum,
    next_offset: int,
    total_size: int,
) -> None:
    if next_offset != total_size:
        raise FileSizeMismatchError(
            filename=filename,
            expected=total_size,
            actual=next_offset,
            message_tpl="Incomplete stream data! Yielded {actual} of {expected} bytes.",
        )

    calculated = hasher.hexdigest()
    if calculated != expected_checksum.value:
        raise HashMismatchError(
            filename=filename,
            algorithm=expected_checksum.algorithm,
            expected=expected_checksum.value,
            actual=calculated,
        )
