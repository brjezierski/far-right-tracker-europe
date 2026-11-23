"""Command line entry point: ``frt update [COUNTRY] [--no-scraping] [--debug]`` or ``frt check``."""

from __future__ import annotations

import argparse
import logging

from frt.build import build
from frt.check import run as run_check
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
    update.add_argument("--debug", action="store_true", help="verbose logging and debug files")

    check = sub.add_parser(
        "check", help="fail if a source link is broken or a country's polls are stale"
    )
    check.add_argument("country", nargs="?", help="only check this country (e.g. Germany)")
    check.add_argument(
        "--max-age-days",
        type=int,
        default=1095,
        help="flag countries whose latest poll is older than this (default: %(default)s)",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO,
        format="%(message)s",
    )
    if args.command == "check":
        return run_check(args.max_age_days, args.country)
    COUNTRIES_DIR.mkdir(parents=True, exist_ok=True)
    build(selected_country=args.country, no_scraping=args.no_scraping)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
