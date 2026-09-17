#!/usr/bin/env python3
"""Carry the published extension repository forward with every Pages snapshot.

Actions artifacts expire. The deployed archive is the durable source for docs-only
deploys; the HTML index bootstraps sites published before the archive existed.
"""

import argparse
from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import tarfile
import tempfile
from urllib.error import HTTPError
from urllib.request import urlopen


ARCHIVE = "extension-repository.tar.gz"
BINARY = re.compile(r"v[\w.-]+/[\w-]+/otlp\.duckdb_extension\.(?:gz|wasm)")


class ExtensionIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.paths = set()

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get("href", "")
        if tag == "a" and href.startswith("../"):
            path = href[3:]
            if BINARY.fullmatch(path):
                self.paths.add(path)


def download(url, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(url, timeout=120) as response, destination.open("wb") as output:
        shutil.copyfileobj(response, output)


def unpack(archive, destination):
    with tarfile.open(archive, "r:gz") as source:
        for member in source.getmembers():
            path = member.name.removeprefix("./")
            if member.isdir():
                continue
            if not member.isfile() or not (
                path in {"extensions/index.html", ".nojekyll"} or BINARY.fullmatch(path)
            ):
                raise ValueError(f"Unexpected extension archive member: {member.name}")
            target = destination / path
            target.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as data, target.open("wb") as output:
                shutil.copyfileobj(data, output)


def validate(repository):
    index = repository / "extensions/index.html"
    parser = ExtensionIndex()
    parser.feed(index.read_text())
    if not parser.paths:
        raise ValueError("Extension index contains no binaries")
    for path in parser.paths:
        binary = repository / path
        if not binary.is_file() or binary.stat().st_size == 0:
            raise ValueError(f"Missing or empty extension binary: {path}")
    return parser.paths


def assemble(destination, *, archive=None, base_url=None):
    with tempfile.TemporaryDirectory() as temporary:
        work = Path(temporary)
        repository = work / "repository"
        repository.mkdir()
        if archive:
            unpack(archive, repository)
        else:
            base_url = base_url.rstrip("/")
            downloaded = work / ARCHIVE
            try:
                download(f"{base_url}/{ARCHIVE}", downloaded)
            except HTTPError as error:
                if error.code != 404:
                    raise
                # One-time migration from the old site: recover every binary
                # listed in its index, including its original DuckDB version.
                index = repository / "extensions/index.html"
                download(f"{base_url}/extensions/", index)
                parser = ExtensionIndex()
                parser.feed(index.read_text())
                for path in sorted(parser.paths):
                    download(f"{base_url}/{path}", repository / path)
                print("Restored extension binaries from the published index")
            else:
                unpack(downloaded, repository)
                print("Restored the published extension archive")

        paths = validate(repository)
        (repository / ".nojekyll").touch()
        # Archive only extension assets, never the site or the archive itself.
        with tarfile.open(work / ARCHIVE, "w:gz") as output:
            for path in [".nojekyll", "extensions/index.html", *sorted(paths)]:
                output.add(repository / path, arcname=path)
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copytree(repository, destination, dirs_exist_ok=True)
        shutil.copyfile(work / ARCHIVE, destination / ARCHIVE)
        print(f"Assembled {len(paths)} extension binaries and a reusable archive")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--archive", type=Path)
    source.add_argument("--base-url")
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    try:
        assemble(args.destination, archive=args.archive, base_url=args.base_url)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(
            1,
            f"Cannot assemble a complete extension repository: {error}. "
            "For a first deployment, run Pages with publish_extension=true.\n",
        )
