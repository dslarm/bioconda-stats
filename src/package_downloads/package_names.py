#! /usr/bin/env python

from asyncio import run
from json import load
from pathlib import Path
from subprocess import DEVNULL, check_call
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, Set

from .common import CHANNELS, escape_path, unescape_path


async def clone_repodata_archive(repodata_archive: Path, channel_url: str) -> None:
    def git(*args: str) -> None:
        check_call(
            ("git", *args),
            cwd=repodata_archive,
            stdout=DEVNULL,
        )

    git(
        "clone",
        "--depth=1",
        "--filter=blob:none",
        "--no-checkout",
        "https://github.com/bioconda/bioconda-repodata-archive",
        ".",
    )
    git("sparse-checkout", "init")
    git("sparse-checkout", "set", "--no-cone", f"repodata/{escape_path(channel_url)}/*/repodata.json")
    git("checkout", "--quiet")


async def extract_package_names(repodata_archive: Path, channel_url: str) -> List[str]:
    names: Set[str] = set()
    for repodata_json_path in repodata_archive.glob(
        f"./repodata/{escape_path(channel_url)}/*/repodata.json",
    ):
        with repodata_json_path.open() as repodata_json_file:
            repodata = load(repodata_json_file)
        for packages_key in ("packages", "packages.conda"):
            names.update((package["name"] for package in repodata[packages_key].values()))
    return sorted(names)


async def retrieve_package_names(channel_url: str) -> List[str]:
    with TemporaryDirectory() as temp_dir:
        repodata_archive = Path(temp_dir)
        await clone_repodata_archive(repodata_archive, channel_url)
        package_names = await extract_package_names(repodata_archive, channel_url)
    return package_names


async def main(args: Optional[List[str]] = None) -> None:
    channels = CHANNELS
    for channel_name, channel_url in channels.items():
        package_names = await retrieve_package_names(channel_url)
        print(
            *(f"{channel_name}::{package_name}" for package_name in package_names),
            sep="\n",
        )


if __name__ == "__main__":
    run(main())
