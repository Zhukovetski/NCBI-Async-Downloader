import sys
from collections.abc import Iterable

from hydrastream.models import Checksum, HydraContext, TypeHash


async def link_feeder(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:
    checksums = None
    for i, link in enumerate(links):
        if expected_checksums is not None:
            checksums = expected_checksums.get(link)
            if checksums and not isinstance(checksums, Checksum):
                checksums = Checksum(algorithm=checksums[0], value=checksums[1])
        else:
            expected_checksums = None
        await ctx.queues.links.put((i, link, checksums))

    for i in range(ctx.tasks.resolvers - 1, -1, -1):
        await ctx.queues.links.put((sys.maxsize - i, "", None))
