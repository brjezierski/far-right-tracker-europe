"""Command line entry point: ``frt update [COUNTRY] [--no-scraping] [--force] [--debug]``."""

from __future__ import annotations

import argparse
import logging

from frt.build import build
from frt.config import COUNTRIES_DIR


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="frt", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    update = sub.add_parser("update", help="scrape polling data and rebuild the dataset")
    update.add_argument("country", nargs="?", help="only process this country (e.g. Germany)")
    update.add_argument(
        "--no-scraping",
        action="store_true",
        help="rebuild JSON outputs from the CSVs already in data/",
    )
    update.add_argument(
        "--force",
        action="store_true",
        help="download and parse every page even if it is unchanged since the last run",
    )
    update.add_argument("--debug", action="store_true", help="verbose logging and debug files")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(message)s",
    )
    COUNTRIES_DIR.mkdir(parents=True, exist_ok=True)
    build(selected_country=args.country, no_scraping=args.no_scraping, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
