from asyncio import sleep
from types import TracebackType
from typing import Any, Dict, Optional, Type

from aiohttp import ClientSession, ClientTimeout
from aiohttp.client_exceptions import ClientResponseError

from .ntp_time import get_ntp_time


class Session:
    def __init__(self) -> None:
        self.client_session = ClientSession(timeout=ClientTimeout(total=15 * 60))
        self.date = get_ntp_time()

    async def __aenter__(self) -> "Session":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.client_session.close()


async def get_and_parse(
    client_session: ClientSession,
    url: str,
    headers: Optional[Dict[str, str]],
    retries: int = 0,
    retry_delay: float = 0.5,
) -> Any:
    async with client_session.get(url, headers=headers) as response:
        while True:
            try:
                response.raise_for_status()
            except ClientResponseError:
                retries -= 1
                if retries < 0:
                    raise
                await sleep(retry_delay)
            else:
                break
        res = await response.json()
    return res
