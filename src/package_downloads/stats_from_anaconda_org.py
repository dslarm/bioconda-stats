#! /usr/bin/env python

from asyncio import run
from functools import partial
from itertools import islice
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, List, Tuple

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
        retries=5,
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
                "top": TOP_DIR,
                "channel": channel,
                "package": package,
                "version": package_file_info["version"],
                "subdir": package_file_info["attrs"]["subdir"],
                "basename": package_file_info["basename"].rsplit("/", 1)[-1],
                "total": max(0, package_file_info["ndownloads"]),
            }
        )
    df = pd.DataFrame(
        sorted(
            downloads,
            key=lambda e: (
                e["top"],
                e["channel"],
                e["package"],
                VersionOrder(e["version"]),
                e["subdir"],
                e["basename"],
                e["total"],
            ),
        )
    )
    return df.set_index(df.loc[:, :"package"].columns.tolist())


async def get_batch_package_download_counts(
    date: str, channel_name: str, package_names: List[str]
) -> Iterable[pd.DataFrame]:
    retries_per_chunk = 5
    retry_delay = 15
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


async def save_counts(counts: Tuple[Tuple[str, ...], pd.DataFrame]) -> None:
    index, totals = counts
    path = Path(BASE_DIR).joinpath(*index[:-1], index[-1] + ".tsv")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(totals.to_csv(sep="\t", lineterminator="\r\n", index=False))


async def save_channel_stats(
    date: str, channel_name: str, package_names: List[str]
) -> pd.DataFrame:
    fetch_count = 0
    totals_list: List[pd.DataFrame] = []
    chunk_size = 500
    for chunk_package_names in chunked_lists(package_names, chunk_size):
        chunk_totals = pd.concat(
            await get_batch_package_download_counts(date, channel_name, chunk_package_names)
        )

        fetch_count += len(chunk_package_names)
        log("save_counts: %s: %d of %d", channel_name, fetch_count, len(package_names))

        grouped = chunk_totals.groupby(chunk_totals.index.names)
        await gather_map(save_counts, grouped)

        totals_list.append(grouped.sum("total"))
    totals = pd.concat(totals_list)
    while True:
        names = totals.index.droplevel(-1).names
        totals.reset_index(inplace=True)
        totals.set_index(names, inplace=True)
        grouped = totals.groupby(totals.index.names)
        if len(totals.index.names) > 1:
            await gather_map(save_counts, grouped)
            totals = grouped.sum("total")
            continue
        return totals


async def main() -> str:
    channels = CHANNELS
    async with Session() as session:
        channel_package_names = {
            channel_name: (await retrieve_package_names(session.client_session, channel_url))
            for channel_name, channel_url in channels.items()
        }
        date = session.date
    totals = pd.DataFrame()
    for channel_name, package_names in channel_package_names.items():
        channel_totals = await save_channel_stats(date, channel_name, package_names)
        totals = pd.concat((totals, channel_totals))
    totals.insert(0, "date", date)
    for index, entry in totals.groupby(totals.index.names[0]):
        path = Path(BASE_DIR).joinpath(index + ".tsv")
        path.write_text(entry.to_csv(sep="\t", lineterminator="\r\n", index=False))
    return date


if __name__ == "__main__":
    basicConfig(level=INFO)
    date = run(main())
    print(date)
