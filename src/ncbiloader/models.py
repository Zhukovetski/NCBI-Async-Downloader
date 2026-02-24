# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


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

    # Метод для красивого отображения в Range header
    def get_header(self) -> dict[str, str]:
        return {"Range": f"bytes={self.current_pos}-{self.end}"}


@dataclass(slots=True)
class File:
    filename: str
    url: str
    content_length: int
    chunks: List[Chunk] = field(default_factory=list[Chunk])

    # Дополнительные поля (на будущее)
    md5: Optional[str] = None
    headers: Dict[str, str] = field(
        default_factory=dict[str, str]
    )  # Например, Cookies или User-Agent

    @property
    def is_complete(self) -> bool:
        """Все ли чанки завершены?"""
        if not self.chunks:
            return False
        return all(c.is_finished for c in self.chunks)

    @property
    def downloaded_size(self) -> int:
        """Сколько байт реально скачано (для Resume)"""
        return sum(c.current_pos - c.start for c in self.chunks)

    @property
    def progress(self) -> float:
        """Процент завершения (0.0 - 100.0)"""
        if self.content_length <= 0:
            return 0.0
        return (self.downloaded_size / self.content_length) * 100

    # --- СЕРИАЛИЗАЦИЯ (Магия для стейтов) ---

    def to_dict(self) -> dict[str, Any]:
        """Превращает объект в словарь для orjson"""
        # dataclasses.asdict работает рекурсивно, но иногда медленно.
        # Можно использовать его, или написать вручную для скорости.
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "File":
        """Восстанавливает объект из словаря (Deserialization)"""
        # Самое важное: восстановить вложенные объекты Chunk!
        chunks_data = data.pop("chunks", [])
        chunks_objs = [Chunk(**c_data) for c_data in chunks_data]

        return cls(chunks=chunks_objs, **data)
