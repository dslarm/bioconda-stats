#! /usr/bin/env python

from asyncio import gather, run
from collections import defaultdict
from datetime import datetime
from functools import partial
from itertools import chain, islice
from json import dumps, loads
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from sys import maxsize
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type

from aiohttp import ClientSession

from .common import BASE_DIR, CHANNELS, DATE_FORMAT
from .ntp_time import get_ntp_time
from .package_names import retrieve_package_names


BASE_PATH = Path(BASE_DIR) / "anaconda.org"
TOPOLOGY = ("channel", "package", "version", "subdir", "basename")
HTTP_HEADERS = {
    "Accept": "application/json",
}

PACKAGE_API_URL_TEMPLATE = "https://api.anaconda.org/package/{channel}/{package}"

log = getLogger(__name__).info


class Session:
    def __init__(self) -> None:
        self.client_session = ClientSession()
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
    client_session: ClientSession, url: str, headers: Dict[str, str]
) -> Any:
    async with client_session.get(url, headers=headers) as response:
        response.raise_for_status()
        res = await response.text()
    return loads(res)


async def get_package_info(
    client_session: ClientSession, channel: str, package_name: str
) -> Dict[str, Any]:
    url = PACKAGE_API_URL_TEMPLATE.format(channel=channel, package=package_name)
    headers = HTTP_HEADERS
    info: Dict[str, Any] = await get_and_parse(client_session, url, headers)
    return info


async def get_package_download_counts(
    session: Session, channel: str, package_name: str
) -> Dict[Tuple[str, ...], int]:
    log("get_package_download_counts: %s::%s", channel, package_name)
    package_info = await get_package_info(session.client_session, channel, package_name)
    downloads: Dict[Tuple[str, ...], int] = dict()
    for package_file_info in package_info["files"]:
        if not "main" in package_file_info["labels"]:
            continue
        if "conda" != package_file_info["type"]:
            continue
        ndownloads: int = package_file_info["ndownloads"]
        version: str = package_file_info["version"]
        subdir: str = package_file_info["attrs"]["subdir"]
        basename: str = package_file_info["basename"].rsplit("/", 1)[-1]
        downloads[(channel, package_name, version, subdir, basename, "dummy")] = max(
            0, ndownloads
        )
    return downloads


def reduce_counts_level(
    counts: Dict[Tuple[str, ...], int]
) -> Dict[Tuple[str, ...], Dict[str, int]]:
    reduced_counts: Dict[Tuple[str, ...], Dict[str, int]] = defaultdict(lambda: {})
    for key, count in counts.items():
        reduced_counts[key[:-1]][key[-1]] = count
    return reduced_counts


async def update_counts(
    date: str,
    counts_per_level: Dict[Tuple[str, ...], int],
    max_sub_category_entries: int = maxsize,
) -> Dict[Tuple[str, ...], int]:
    totals = {}
    for key, counts in reduce_counts_level(counts_per_level).items():
        total = sum(counts.values())
        totals[key] = total
        path = Path(BASE_PATH).joinpath(*key[:-1], f"{key[-1]}.json")
        if path.is_file():
            data = loads(path.read_text())
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = dict(zip(TOPOLOGY, key))
            data["downloads_per_date"] = []
        downloads_per_date = data["downloads_per_date"]
        downloads_per_date.append({"date": date, "total": total})
        # Ensure sorted by date (should already be the case, but just to make sure).
        downloads_per_date = sorted(
            downloads_per_date,
            key=lambda entry: (datetime.strptime(entry["date"], DATE_FORMAT), entry["total"]),
        )
        # Remove redundant (no count change / same date) information
        cleaned_downloads_per_date = []
        for prev_, curr_, next_ in zip(
            chain([{"date": "0000-01-01", "total": -1}], downloads_per_date),
            downloads_per_date,
            chain(
                islice(downloads_per_date, 1, maxsize),
                [{"date": "9999-12-31", "total": maxsize}],
            ),
        ):
            if prev_["total"] == curr_["total"] == next_["total"]:
                continue
            if curr_["date"] == next_["date"]:
                continue
            cleaned_downloads_per_date.append(curr_)
        data["downloads_per_date"] = cleaned_downloads_per_date
        if len(key) < len(TOPOLOGY):
            sub_category = TOPOLOGY[len(key)]
            downloads_per_sub_category = sorted(
                [
                    {sub_category: sub_category_key, "total": total}
                    for sub_category_key, total in counts.items()
                ],
                key=lambda entry: next(iter(entry.items()))[::-1],
            )
            data[f"downloads_per_{sub_category}"] = downloads_per_sub_category[
                :max_sub_category_entries
            ]
        json = dumps(data, indent=0, separators=(",", ":"), sort_keys=True)
        path.write_text(json)
    return totals


async def update_package_counts(
    channel: str, package_name: str, date: str, counts_per_basename: Dict[Tuple[str, ...], int]
) -> Dict[Tuple[str, ...], int]:
    log("update_package_counts: %s::%s", channel, package_name)
    counts_per_level = counts_per_basename
    for _ in range(len(TOPOLOGY) - 1):
        counts_per_level = await update_counts(date, counts_per_level)
    return counts_per_level


async def save_package_stats(
    session: Session, channel: str, package_name: str
) -> Tuple[str, int]:
    counts_per_basename = await get_package_download_counts(session, channel, package_name)
    counts_per_version = await update_package_counts(
        channel, package_name, session.date, counts_per_basename
    )
    return (package_name, sum(counts_per_version.values()))


async def save_channel_stats(
    session: Session, channel_name: str, package_names: List[str]
) -> None:
    counts_per_package: Dict[Tuple[str, ...], int] = {
        (channel_name, package_name): total
        for package_name, total in await gather(
            *map(partial(save_package_stats, session, channel_name), package_names)
        )
    }
    await update_counts(session.date, counts_per_package, max_sub_category_entries=200)


async def main() -> None:
    channels = CHANNELS
    channel_package_names = {
        channel_name: await retrieve_package_names(channel_url)
        for channel_name, channel_url in channels.items()
    }
    async with Session() as session:
        for channel_name, package_names in channel_package_names.items():
            await save_channel_stats(session, channel_name, package_names)
        return session.date


if __name__ == "__main__":
    basicConfig(level=INFO)
    date = run(main())
    print(date)
