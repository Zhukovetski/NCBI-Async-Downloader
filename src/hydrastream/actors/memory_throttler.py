import asyncio
import sys

from hydrastream.models import Chunk, Envelope, StopMsg, my_dataclass


@my_dataclass
class MemoryThrottler:
    chunk_inbox: asyncio.PriorityQueue[Envelope[Chunk | StopMsg]]
    chunk_outbox: asyncio.PriorityQueue[Envelope[Chunk | StopMsg]]
    credit_inbox: asyncio.Queue[int]

    budget: int

    async def run(self) -> None:

        while True:
            envelope = await self.chunk_inbox.get()
            msg = envelope.payload

            match msg:
                case Chunk() as pending_chunk:
                    if pending_chunk.size > self.budget:
                        credit = await self.credit_inbox.get()
                        self.budget += credit
                        continue

                    self.budget -= pending_chunk.size
                    await self.chunk_outbox.put(envelope)

                case StopMsg():
                    await self.chunk_outbox.put(
                        Envelope(sort_key=(sys.maxsize,), payload=StopMsg())
                    )
                    break
