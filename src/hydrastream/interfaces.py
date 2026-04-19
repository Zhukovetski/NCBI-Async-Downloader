from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from typing_extensions import Buffer, runtime_checkable

if TYPE_CHECKING:
    from hydrastream.models import Checksum, File, NetworkState, TypeHash


@runtime_checkable
class StorageBackend(Protocol):
    def allocate_space(self, filename: str, size: int) -> str | None: ...

    def open_file(self, filename: str) -> Any: ...

    async def write_chunk_data(
        self, fd_or_conn: Any, data: bytearray, offset: int
    ) -> None: ...

    def close_file(self, fd_or_conn: Any) -> None: ...

    def delete_file(self, filename: str) -> None: ...

    def save_state(self, file_obj: File) -> None: ...

    def load_state(self, filename: str) -> tuple[File | None, int]: ...

    def delete_state(self, filename: str) -> None: ...

    def verify_size(self, filename: str, expected_size: int) -> bool: ...

    async def verify_file_hash(
        self, filename: str, expected_checksum: str, algorithm: TypeHash
    ) -> None: ...

    def get_unique_path(self, file_path: Any) -> Any: ...

    def get_state_path(self, filename: str) -> Any: ...


@runtime_checkable
class HashProvider(Protocol):
    async def resolve(
        self, ctx: NetworkState, url: str, filename: str
    ) -> Checksum | None: ...


@runtime_checkable
class Hasher(Protocol):
    def update(self, data: Buffer, /) -> None: ...
    def hexdigest(self) -> str: ...
