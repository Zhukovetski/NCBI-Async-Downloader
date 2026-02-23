from dataclasses import dataclass


@dataclass
class Chunk:
    url: str
    start: int
    end: int
    current_pos: int
    filename: str
