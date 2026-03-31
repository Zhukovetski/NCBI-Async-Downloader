# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.
from _hashlib import HASH

from .models import File, HydraContext


def save_all_states(ctx: HydraContext, files: dict[int, File]) -> None:
    for file in list(files.values()):
        if file.chunks and not all(c.current_pos > c.end for c in (file.chunks or [])):
            ctx.fs.save_state(file)


def verify_stream(
    md5_hasher: HASH, expected_checksum: str, next_offset: int, total_size: int
) -> None:
    calculated = md5_hasher.hexdigest()
    if calculated != expected_checksum:
        err_msg = (
            f"CRITICAL: Stream Integrity Check Failed!\n"
            f"Expected MD5: {expected_checksum}\n"
            f"Got MD5:      {calculated}"
        )
        raise ValueError(err_msg)

    if next_offset != total_size:
        raise ValueError(
            f"Incomplete stream data! Yielded {next_offset} bytes,"
            f" but expected {total_size} bytes."
        )
