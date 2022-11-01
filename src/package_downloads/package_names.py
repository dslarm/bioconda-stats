#! /usr/bin/env python

from asyncio import gather, run
from functools import partial
from typing import Any, Collection, Dict, List, Optional, Set

from aiohttp import ClientSession

from .common import CHANNELS, SUBDIRS, escape_path, unescape_path
from .download import Session, get_and_parse


async def download_repodata(
    client_session: ClientSession,
    channel_url: str,
    subdir: str,
) -> Any:
    return await get_and_parse(
        client_session,
        f"{channel_url}/{subdir}/repodata.json",
        None,
        retries=10,
        retry_delay=5,
    )


async def extract_package_names(
    client_session: ClientSession,
    channel_url: str,
    subdir: str,
) -> Set[str]:
    names: Set[str] = set()
    repodata = await download_repodata(client_session, channel_url, subdir)
    for packages_key in ("packages", "packages.conda"):
        names.update((package["name"] for package in repodata[packages_key].values()))
    return names


async def retrieve_package_names(
    client_session: ClientSession,
    channel_url: str,
    subdirs: Collection[str] = SUBDIRS,
) -> List[str]:
    package_names: Set[str] = set()
    for names in await gather(
        *map(
            partial(extract_package_names, client_session, channel_url),
            subdirs,
        )
    ):
        package_names.update(names)
    return sorted(package_names)


async def main(args: Optional[List[str]] = None) -> None:
    channels = CHANNELS
    async with Session() as session:
        for channel_name, channel_url in channels.items():
            package_names = await retrieve_package_names(session.client_session, channel_url)
            print(
                *(f"{channel_name}::{package_name}" for package_name in package_names),
                sep="\n",
            )


if __name__ == "__main__":
    run(main())
