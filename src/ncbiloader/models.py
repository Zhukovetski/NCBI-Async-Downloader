# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from dataclasses import dataclass, field

import orjson


@dataclass(slots=True)
class Chunk:
    filename: str
    start: int
    end: int
    current_pos: int

    @property
    def is_finished(self) -> bool:
        return self.current_pos > self.end

    @property
    def size(self) -> int:
        return self.end - self.start + 1

    @property
    def remaining(self) -> int:
        return max(0, self.end - self.current_pos + 1)

    def get_header(self) -> dict[str, str]:
        return {"Range": f"bytes={self.current_pos}-{self.end}"}


@dataclass
class File:
    filename: str
    url: str
    content_length: int
    chunk_size: int
    chunks: list[Chunk] = field(default_factory=list[Chunk])

    # Дополнительные поля (на будущее)
    md5: str | None = None
    headers: dict[str, str] = field(
        default_factory=dict[str, str]
    )  # Например, Cookies или User-Agent

    def __post_init__(self) -> None:
        if self.chunks:
            return

        if self.chunk_size <= 0:
            raise ValueError("Chunk size must be positive")
        part_count = -(-self.content_length // self.chunk_size)

        for i in range(part_count):
            start = i * self.chunk_size
            end = min((i + 1) * self.chunk_size - 1, self.content_length - 1)

            self.chunks.append(
                Chunk(
                    start=start,
                    end=end,
                    current_pos=start,
                    filename=self.filename,
                )
            )

    @property
    def is_complete(self) -> bool:
        """Все ли чанки завершены?"""
        if not self.chunks:
            return False
        return all(c.is_finished for c in self.chunks)

    @property
    def downloaded_size(self) -> int:
        """Сколько байт реально скачано (для Resume)"""
        return sum(c.current_pos - c.start for c in (self.chunks or []))

    @property
    def progress(self) -> float:
        """Процент завершения (0.0 - 100.0)"""
        if self.content_length <= 0:
            return 0.0
        return (self.downloaded_size / self.content_length) * 100

    # --- СЕРИАЛИЗАЦИЯ (Магия для стейтов) ---

    def to_json(self) -> bytes:
        """Превращает объект в словарь для orjson"""
        # dataclasses.asdict работает рекурсивно, но иногда медленно.
        # Можно использовать его, или написать вручную для скорости.
        return orjson.dumps(
            self, option=orjson.OPT_SERIALIZE_DATACLASS | orjson.OPT_INDENT_2
        )

    @classmethod
    def from_json(cls, content: bytes) -> File:
        """Восстанавливает объект из словаря (Deserialization)"""
        data = orjson.loads(content)
        chunks_data = data.get("chunks", [])
        chunks_data["chunks"] = [Chunk(**c_data) for c_data in chunks_data]

        return cls(**data)
