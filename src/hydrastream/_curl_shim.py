# src/hydrastream/_curl_shim.py
# pyright: reportMissingTypeStubs=false
# type: ignore[reportGeneralTypeIssues]

from collections.abc import AsyncGenerator
from typing import Any

from curl_cffi.requests import AsyncSession, RequestsError, Response
from curl_cffi.requests.session import HttpMethod


async def aiter_bytes(
    response: Response, chunk_size: int
) -> AsyncGenerator[bytes, None]:
    iterator = response.aiter_content(chunk_size=chunk_size)

    async for chunk in iterator:
        yield chunk


async def request(
    client: AsyncSession[Response],
    method: HttpMethod | str,
    url: str,
    **kwargs: Any,  # noqa: ANN401
) -> Response:
    return await client.request(method, url, **kwargs)


def get_error_response(e: RequestsError) -> Response | None:
    return e.response
