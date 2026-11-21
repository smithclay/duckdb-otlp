#!/usr/bin/env python3
"""Check every internal link and anchor in the built documentation site.

mdBook does not validate links, and a page moved or deleted without updating its
inbound links would otherwise ship as a 404. Run after `mdbook build`:

    python3 scripts/check_site_links.py site/dist

Relative hrefs are resolved against the page; root-relative ones must start with
the Pages base path. A directory link resolves to its index.html. Exits 1 on any
broken target or missing `#fragment`.
"""

import argparse
import html
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

BASE_PATH = "/duckdb-otlp/"
# The print view and table of contents repeat every page's links; the WASM demo is a
# standalone app whose assets are loaded by script, not by href.
SKIP_PAGES = {"print.html", "toc.html"}
SKIP_DIRS = {"wasm-demo"}

HREF = re.compile(r'(?:href|src)="([^"]+)"')
ID = re.compile(r'\bid="([^"]+)"')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("site", type=Path, help="built site directory (mdBook build-dir)")
    root = parser.parse_args().site.resolve()

    anchors: dict[Path, set[str]] = {}
    problems = []
    for page in sorted(root.rglob("*.html")):
        if page.name in SKIP_PAGES or SKIP_DIRS.intersection(page.relative_to(root).parts):
            continue
        for raw in HREF.findall(page.read_text(errors="ignore")):
            href = html.unescape(raw)
            url = urlsplit(href)
            if url.scheme or href.startswith(("//", "mailto:")):
                continue
            if url.path.startswith(BASE_PATH):
                target = root / unquote(url.path[len(BASE_PATH) :])
            elif url.path.startswith("/"):
                problems.append(f"{page.relative_to(root)}: {href} (outside {BASE_PATH})")
                continue
            elif url.path:
                target = page.parent / unquote(url.path)
            else:
                target = page
            target = target.resolve()
            if target.is_dir():
                target /= "index.html"
            if not target.exists():
                problems.append(f"{page.relative_to(root)}: {href} (no such page)")
            elif url.fragment and target.suffix == ".html":
                if target not in anchors:
                    anchors[target] = set(ID.findall(target.read_text(errors="ignore")))
                if url.fragment not in anchors[target]:
                    problems.append(f"{page.relative_to(root)}: {href} (no such anchor)")

    for problem in problems:
        print(problem, file=sys.stderr)
    print(f"{len(problems)} broken link(s)", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
