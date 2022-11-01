#! /usr/bin/env python

from asyncio import gather, run
from collections import defaultdict, OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from itertools import chain, islice
from json import dumps, loads
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from sys import maxsize
from typing import Any, Dict, List, OrderedDict as ODict, Tuple

from aiohttp import ClientSession

from .common import BASE_DIR, CHANNELS, DATE_FORMAT
from .download import Session, get_and_parse
from .package_names import retrieve_package_names


BASE_PATH = Path(BASE_DIR) / "anaconda.org"
TOPOLOGY = ("channel", "package", "version", "subdir", "basename")
HTTP_HEADERS = {
    "Accept": "application/json",
}

PACKAGE_API_URL_TEMPLATE = "https://api.anaconda.org/package/{channel}/{package}"

log = getLogger(__name__).info


async def get_package_info(
    client_session: ClientSession, channel: str, package_name: str
) -> Dict[str, Any]:
    url = PACKAGE_API_URL_TEMPLATE.format(channel=channel, package=package_name)
    headers = HTTP_HEADERS
    info: Dict[str, Any] = await get_and_parse(
        client_session,
        url,
        headers,
        retries=5,
        retry_delay=0.5,
    )
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
        downloads[(channel, package_name, version, subdir, basename)] = max(0, ndownloads)
    return downloads


def get_path(path_tuple: Tuple[str, ...]) -> Path:
    return Path(BASE_PATH).joinpath(*path_tuple[:-1], f"{path_tuple[-1]}.json")


def from_list_of_dicts(
    input_list: List[Dict[str, Any]], key_str: str, value_str: str
) -> Dict[str, Any]:
    return {entry[key_str]: entry[value_str] for entry in input_list}


async def load_old_counts(path_tuple: Tuple[str, ...]) -> Dict[str, int]:
    path = get_path(path_tuple)
    if path.is_file():
        return from_list_of_dicts(
            loads(path.read_text()).get("downloads_per_date", []), "date", "total"
        )
    return {}


def parse_date(date: str) -> datetime:
    return datetime.strptime(date, DATE_FORMAT)


def sort_by_date(counts_per_date: Dict[str, Any]) -> List[Tuple[str, Any]]:
    return sorted(
        counts_per_date.items(),
        key=lambda date__total: (parse_date(date__total[0]), date__total[1]),
    )


def dedup_counts_per_date(counts_per_date: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure sorted by date.
    counts_per_date_list = sort_by_date(counts_per_date)
    # Remove redundant (no count change / same date) information.
    deduped_counts_per_date = []
    for prev_, curr_, next_ in zip(
        chain([("0000-01-01", -1)], counts_per_date_list),
        counts_per_date_list,
        chain(islice(counts_per_date_list, 1, maxsize), [("9999-12-31", maxsize)]),
    ):
        if prev_[1] == curr_[1] == next_[1]:
            continue
        if curr_[0] == next_[0]:
            continue
        deduped_counts_per_date.append(curr_)
    return OrderedDict(deduped_counts_per_date)


def as_list(
    input_dict: Dict[Any, Any],
    key_str: str,
    value_str: str,
) -> List[Dict[str, Any]]:
    output_list = []
    for key, value in input_dict.items():
        output_list.append({key_str: key, value_str: value})
    return output_list


async def save_json(path_tuple: Tuple[str, ...], data: Dict[str, Any]) -> None:
    path = get_path(path_tuple)
    path.parent.mkdir(parents=True, exist_ok=True)
    output = data.copy()
    output.update(dict(zip(TOPOLOGY, path_tuple)))
    json = dumps(output, indent=0, separators=(",", ":"), sort_keys=True)
    path.write_text(json)


async def update_basename_counts(
    current_date: str,
    current_basename_totals: Dict[Tuple[str, ...], int],
) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    basename_counts_per_date = {}
    for key, total in current_basename_totals.items():
        counts_per_date = await load_old_counts(key)
        counts_per_date.update({current_date: total})
        counts_per_date = dedup_counts_per_date(counts_per_date)
        await save_json(key, {"downloads_per_date": as_list(counts_per_date, "date", "total")})
        basename_counts_per_date[key] = counts_per_date
    return basename_counts_per_date


def split_off_level_counts(
    counts: Dict[Tuple[str, ...], Dict[str, Any]]
) -> Dict[Tuple[str, ...], Dict[str, Dict[str, Any]]]:
    reduced_counts: Dict[Tuple[str, ...], Dict[str, Any]] = defaultdict(dict)
    for key, counts_for_level in counts.items():
        reduced_counts[key[:-1]][key[-1]] = counts_for_level
    return reduced_counts


def sort_by_date_and_counts(
    counts_per_date: Dict[str, Dict[str, int]]
) -> ODict[str, ODict[str, int]]:
    return OrderedDict(
        sorted(
            (
                (
                    date,
                    OrderedDict(
                        sorted(
                            ((key, total) for key, total in key_totals.items()),
                            key=lambda key_total: key_total[1],
                        )
                    ),
                )
                for date, key_totals in counts_per_date.items()
            ),
            key=lambda key_value: parse_date(key_value[0]),
        )
    )


def get_sub_counts_per_date(
    all_sub_counts_per_date: Dict[str, Dict[str, int]],
) -> ODict[str, ODict[str, int]]:
    counts_per_date: Dict[str, Dict[str, int]] = defaultdict(dict)
    for sub_key, sub_counts_per_date in all_sub_counts_per_date.items():
        for date, total in sub_counts_per_date.items():
            counts_per_date[date][sub_key] = total
    return sort_by_date_and_counts(counts_per_date)


def get_recent_counts(
    session_date: str,
    max_sub_level_entries: int,
    max_days: int,
    counts_per_date: ODict[str, ODict[str, int]],
) -> ODict[str, ODict[str, int]]:
    recent_counts_per_date: Dict[str, Dict[str, int]] = defaultdict(dict)
    recent_total_counts: Dict[str, int] = {}
    min_date = parse_date(session_date) - timedelta(days=max_days)
    total_counts = counts_per_date[session_date]
    prev_totals: Dict[str, int] = defaultdict(int)
    for date, key_totals in counts_per_date.items():
        if parse_date(date) < min_date:
            continue
        for key, total in key_totals.items():
            if total > prev_totals[key]:
                if key not in recent_total_counts:
                    recent_total_counts[key] = total_counts[key] - total
                recent_counts_per_date[date][key] = total
                prev_totals[key] = total
    recent_selection = sorted(
        recent_total_counts.keys(), key=lambda key: recent_total_counts[key]
    )[-max_sub_level_entries:]
    selected_recent_counts_per_date = {}
    for date, key_totals in recent_counts_per_date.items():
        counts_at_date = {}
        for key, total in key_totals.items():
            if key in recent_selection:
                counts_at_date[key] = total
        if counts_at_date:
            selected_recent_counts_per_date[date] = counts_at_date
    return sort_by_date_and_counts(selected_recent_counts_per_date)


def get_summed_up_counts_per_date(
    session_date: str,
    ordered_counts_per_date: ODict[str, ODict[str, int]],
) -> Dict[str, int]:
    tmp: Dict[str, int] = ordered_counts_per_date[session_date].copy()
    summed_up_total = sum(tmp.values())
    summed_up_counts_per_date = {}
    for date, counts_at_date in reversed(ordered_counts_per_date.items()):
        for key, total in counts_at_date.items():
            summed_up_total -= tmp[key] - total
            tmp[key] = total
        summed_up_counts_per_date[date] = summed_up_total
    return dedup_counts_per_date(summed_up_counts_per_date)


async def update_counts(
    session_date: str,
    max_sub_level_entries: int,
    max_days: int,
    level_counts_per_date: Dict[Tuple[str, ...], Dict[str, Any]],
) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    next_level_counts_per_date = {}
    for key, all_sub_counts_per_date in split_off_level_counts(level_counts_per_date).items():
        sub_level = TOPOLOGY[len(key)]
        ordered_counts_per_date = get_sub_counts_per_date(all_sub_counts_per_date)
        ordered_recent_counts_per_date = get_recent_counts(
            session_date, max_sub_level_entries, max_days, ordered_counts_per_date
        )
        summed_up_counts_per_date = get_summed_up_counts_per_date(
            session_date, ordered_counts_per_date
        )
        downloads_per_sub_level = dict(
            sorted(
                ordered_counts_per_date[session_date].items(),
                key=lambda sub_key_total: sub_key_total[1],
            )[-max_sub_level_entries:]
        )
        data = {
            "downloads_per_date": as_list(summed_up_counts_per_date, "date", "total"),
            f"downloads_per_{sub_level}": as_list(downloads_per_sub_level, sub_level, "total"),
            "recent_downloads": as_list(
                {
                    date: as_list(recent_counts_at_date, sub_level, "total")
                    for date, recent_counts_at_date in ordered_recent_counts_per_date.items()
                },
                "date",
                f"downloads_per_{sub_level}",
            ),
        }
        await save_json(key, data)
        next_level_counts_per_date[key] = summed_up_counts_per_date
    return next_level_counts_per_date


async def save_package_stats(
    session: Session,
    max_sub_level_entries: int,
    max_days: int,
    channel: str,
    package_name: str,
) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    current_basename_totals = await get_package_download_counts(session, channel, package_name)
    log("update_basename_counts: %s::%s", channel, package_name)
    session_date = session.date
    basename_counts_per_date = await update_basename_counts(
        session_date, current_basename_totals
    )
    level_counts_per_date = basename_counts_per_date
    for _ in range(len(TOPOLOGY) - 2):
        level_counts_per_date = await update_counts(
            session_date, max_sub_level_entries, max_days, level_counts_per_date
        )
    return level_counts_per_date


async def save_channel_stats(
    session: Session, channel_name: str, package_names: List[str]
) -> None:
    max_sub_level_entries = 50
    max_days = 62
    all_package_counts_per_date: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    for package_counts_per_date in await gather(
        *map(
            partial(save_package_stats, session, max_sub_level_entries, max_days, channel_name),
            package_names,
        )
    ):
        all_package_counts_per_date.update(package_counts_per_date)
    await update_counts(
        session.date,
        max_sub_level_entries,
        max_days,
        all_package_counts_per_date,
    )


async def main() -> str:
    channels = CHANNELS
    async with Session() as session:
        channel_package_names = {
            channel_name: (await retrieve_package_names(session.client_session, channel_url))
            for channel_name, channel_url in channels.items()
        }
        for channel_name, package_names in channel_package_names.items():
            await save_channel_stats(session, channel_name, package_names)
        return session.date


if __name__ == "__main__":
    basicConfig(level=INFO)
    date = run(main())
    print(date)
