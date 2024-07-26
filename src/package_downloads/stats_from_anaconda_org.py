#! /usr/bin/env python

from asyncio import run
from collections import defaultdict
from functools import partial
from itertools import islice
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, List
import re

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError
import pandas as pd

from .common import BASE_DIR, CHANNELS, gather_map
from .download import Session, get_and_parse
from .package_names import retrieve_package_names
from ._vendor.conda.models.version import VersionOrder


TOP_DIR = "anaconda.org"
HTTP_HEADERS = {
    "Accept": "application/json",
}
PACKAGE_API_URL_TEMPLATE = "https://api.anaconda.org/package/{channel}/{package}"
PACKAGE_EXTENSION_RE = re.compile("\.tar\.bz2$|\.conda$")

logger = getLogger(__name__)
log = logger.info


def chunked_lists(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


async def fetch_package_info(
    client_session: ClientSession, channel: str, package: str
) -> Dict[str, Any]:
    url = PACKAGE_API_URL_TEMPLATE.format(channel=channel, package=package)
    headers = HTTP_HEADERS
    info: Dict[str, Any] = await get_and_parse(
        client_session,
        url,
        headers,
        retries=2,
        retry_delay=1,
    )
    return info


async def fetch_package_download_counts(
    session: Session, channel: str, package: str
) -> pd.DataFrame:
    logger.debug("fetch_package_download_counts: %s::%s", channel, package)
    package_info = await fetch_package_info(session.client_session, channel, package)
    downloads = []
    for package_file_info in package_info["files"]:
        if "main" not in package_file_info["labels"]:
            continue
        if "conda" != package_file_info["type"]:
            continue
        downloads.append(
            {
                "package": package,
                "version": package_file_info["version"],
                "subdir": package_file_info["attrs"]["subdir"],
                # "build": package_file_info["attrs"]["build"],
                # "extension": PACKAGE_EXTENSION_RE.search(package_file_info["basename"])[0],
                "total": max(0, package_file_info["ndownloads"]),
            }
        )
    return pd.DataFrame(
        sorted(
            downloads,
            key=lambda e: (
                e["package"],
                VersionOrder(e["version"]),
                # VersionOrder can be ambiguous (e.g., "1.1" == "1.01"), so compare by str, too.
                e["version"],
                e["subdir"],
                # e["build"],
                # e["extension"],
            ),
        )
    )


async def get_batch_package_download_counts(
    date: str, channel_name: str, package_names: List[str]
) -> Iterable[pd.DataFrame]:
    retries_per_chunk = 2
    retry_delay = 60
    retry = 0
    while True:
        try:
            async with Session(date=date) as session:
                return await gather_map(
                    partial(fetch_package_download_counts, session, channel_name),
                    package_names,
                )
        except ClientError:
            if retry > retries_per_chunk:
                raise
            retry += 1
            logger.exception(
                "Got a ClientError; "
                f"Delaying further exeuction by {retry_delay}s (retry: {retry})..."
            )
            # This is time.sleep, not asyncio.sleep, i.e., we halt everything.
            sleep(retry_delay)


async def get_channel_stats(
    date: str, channel_name: str, package_names: List[str]
) -> pd.DataFrame:
    chunk_size = 50
    inter_chunk_delay = 0.5
    fetch_count = 0
    stats_list: List[pd.DataFrame] = []
    for chunk_package_names in chunked_lists(package_names, chunk_size):
        stats_list.extend(
            await get_batch_package_download_counts(date, channel_name, chunk_package_names)
        )
        current_chunk_size = len(chunk_package_names)
        fetch_count += current_chunk_size
        log("get_channel_stats: %s: %d of %d", channel_name, fetch_count, len(package_names))
        if current_chunk_size == chunk_size:
            # This is time.sleep, not asyncio.sleep, i.e., we halt everything.
            sleep(inter_chunk_delay)
    return pd.concat(stats_list)


def read_tsv(path: Path, **kwargs: Any) -> pd.DataFrame:
    return pd.read_csv(path, sep="\t", dtype=defaultdict(lambda: str, total=int))


def write_tsv(path: Path, data_frame: pd.DataFrame) -> None:
    data_frame.to_csv(path, sep="\t", lineterminator="\n", index=True)


async def save_packages_stats(channel_dir: Path, totals: pd.DataFrame) -> None:
    log("save_packages_stats: %s", channel_dir.name)
    packages_totals = totals.groupby("package", sort=True)
    write_tsv(channel_dir / "packages.tsv", packages_totals.sum("total"))

    versions_dir = channel_dir / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)
    platforms_dir = channel_dir / "platforms"
    platforms_dir.mkdir(parents=True, exist_ok=True)
    for package, package_totals in packages_totals:
        version_totals = package_totals.groupby(["version"], sort=False)
        write_tsv(versions_dir / f"{package}.tsv", version_totals.sum("total"))
        subdir_totals = package_totals.groupby(["subdir"], sort=False)
        write_tsv(platforms_dir / f"{package}.tsv", subdir_totals.sum("total"))


async def save_historic_channel_stats(
    date: str, channel_dir: Path, totals: pd.DataFrame
) -> None:
    subdirs_totals = totals.groupby("subdir", sort=True).sum(numeric_only=True)["total"]
    total_dict = {"date": date, "total": totals["total"].sum()}
    total_dict.update(subdirs_totals.to_dict())
    channel_totals = pd.DataFrame([total_dict])
    channel_tsv = channel_dir / "channel.tsv"
    if channel_tsv.exists():
        channel_totals = pd.concat([read_tsv(channel_tsv), channel_totals])
    channel_totals.set_index("date", inplace=True)
    write_tsv(channel_tsv, channel_totals)


async def save_channel_stats(date: str, channel_name: str, package_names: List[str]) -> None:
    totals = await get_channel_stats(date, channel_name, package_names)

    log("save_channel_stats: %s: entries %d", channel_name, len(totals))

    channel_dir = Path(BASE_DIR) / TOP_DIR / channel_name
    channel_dir.mkdir(parents=True, exist_ok=True)

    await save_historic_channel_stats(date, channel_dir, totals)

    subdirs_totals = totals.groupby("subdir", sort=True)
    write_tsv(channel_dir / "subdirs.tsv", subdirs_totals.sum("total"))

    await save_packages_stats(channel_dir, totals)


async def main() -> str:
    channels = CHANNELS
    async with Session() as session:
        channel_package_names = {
            channel_name: (await retrieve_package_names(session.client_session, channel_url))
            for channel_name, channel_url in channels.items()
        }
        date = session.date
    for channel_name, package_names in channel_package_names.items():
        await save_channel_stats(date, channel_name, package_names[1:100])
    return date


if __name__ == "__main__":
    basicConfig(level=INFO)
    date = run(main())
    print(date)
