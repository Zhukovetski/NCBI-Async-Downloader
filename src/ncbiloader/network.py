# network.py
import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any, NotRequired, TypedDict, Unpack

import httpx
from aiolimiter import AsyncLimiter


class RequestOptions(TypedDict):
    params: NotRequired[dict[str, Any] | None]
    headers: NotRequired[dict[str, str] | None]
    json: NotRequired[Any]
    data: NotRequired[Any]
    timeout: NotRequired[float | None]


class NetworkClient:
    def __init__(self, threads: int, timeout: float = 10.0, http2: bool = True) -> None:
        self.limiter = AsyncLimiter(threads * 2, 1)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, read=5.0),
            http2=http2,
            follow_redirects=True,
            headers={"Accept-Encoding": "identity", "User-Agent": "Bio-Reactor/1.0"},
        )

    async def safe_request(
        self, method: str, url: str, **kwargs: Unpack[RequestOptions]
    ) -> httpx.Response | None:
        """Делает запрос с ретраями"""
        for i in range(3):
            async with self.limiter:
                try:
                    resp = await self.client.request(method, url, **kwargs)
                    if resp.status_code >= 400:
                        if resp.status_code in {408, 429, 500, 502, 503, 504}:
                            await asyncio.sleep(2**i)
                            continue
                        return None  # Фатальная ошибка (404)
                    return resp
                except httpx.RequestError:
                    await asyncio.sleep(2**i)
        return None

    @contextlib.asynccontextmanager
    async def stream_chunk(
        self, url: str, headers: dict[str, str], timeout: int
    ) -> AsyncIterator[httpx.Response]:
        """
        Открывает потоковое соединение и работает как контекстный менеджер.
        """
        async with asyncio.timeout(timeout):
            async with self.client.stream("GET", url, headers=headers) as response:
                if response.status_code >= 400:
                    raise Exception(f"HTTP Error: {response.status_code} for {url}")

                yield response

    async def close(self) -> None:
        await self.client.aclose()
