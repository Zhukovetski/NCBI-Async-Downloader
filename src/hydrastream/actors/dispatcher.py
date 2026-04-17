# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import sys

from hydrastream.models import Chunk, HydraContext


async def chunk_dispatcher(ctx: HydraContext) -> None:
    while True:
        priority_index, file = await ctx.queues.dispatch_file.get()
        if priority_index == sys.maxsize or file is None:
            break
        if ctx.stream:
            await ctx.queues.file_discovery.put(priority_index)
        file.create_chunks()
        for c in file.chunks:
            if c.current_pos <= c.end:
                await ctx.queues.chunk.put(
                    (priority_index, c) if ctx.stream else (c, priority_index)
                )

        ctx.current_files_id.add(priority_index)

        async with ctx.sync.current_files:
            await ctx.sync.current_files.wait_for(
                lambda: (
                    not ctx.current_files_id
                    if ctx.stream
                    else len(ctx.current_files_id) < ctx.config.threads
                )
            )

    c = Chunk(current_pos=sys.maxsize, start=sys.maxsize, end=sys.maxsize)

    for i in range(ctx.tasks.workers - 1, -1, -1):
        await ctx.queues.chunk.put(
            (sys.maxsize - i, c) if ctx.stream else (c, sys.maxsize - i)
        )
