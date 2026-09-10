"""Download US public holidays from Nager.Date into the bronze layer.

Run inside the Airflow container:
    python -m ingestion.holidays --year 2025

Files land in:
    data/bronze/holidays/year=YYYY/us_holidays_YYYY.json
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from ingestion.common import DATA_DIR, TIMEOUT, atomic_output, get_session, setup_logging
from ingestion.db import record_run

API_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"

log = logging.getLogger(__name__)


def build_path(year: int, country: str = "US") -> Path:
    return DATA_DIR / "bronze" / "holidays" / f"year={year}" / f"{country.lower()}_holidays_{year}.json"


def fetch_year(year: int, country: str = "US") -> bytes:
    """Call the API for one year and return the raw response body."""
    url = API_URL.format(year=year, country=country)
    log.info("Requesting %s", url)
    resp = get_session().get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


def validate(payload: object, year: int) -> None:
    """Refuse to save a response that is empty or not what we asked for."""
    if not isinstance(payload, list) or not payload:
        raise ValueError("Expected a non-empty list of holidays")
    for holiday in payload:
        if not {"date", "name"} <= holiday.keys():
            raise ValueError(f"Holiday entry is missing date or name: {holiday}")
        if not holiday["date"].startswith(f"{year}-"):
            raise ValueError(f"Holiday {holiday['name']} is dated {holiday['date']}, not in {year}")


def download_year(year: int, country: str = "US", force: bool = False) -> Path:
    """Fetch, validate and save one year. Safe to run again."""
    dest = build_path(year, country)
    if dest.exists() and not force:
        log.info("Skip: %s already downloaded", dest.name)
        record_run("holidays", str(year), dest, "skipped", len(json.loads(dest.read_bytes())))
        return dest

    raw = fetch_year(year, country)
    payload = json.loads(raw)
    validate(payload, year)

    with atomic_output(dest) as tmp:
        tmp.write_bytes(raw)
    log.info("Saved %s (%d holidays)", dest, len(payload))
    record_run("holidays", str(year), dest, "downloaded", len(payload))
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download one year of US public holidays.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--force", action="store_true", help="download again even if the file exists")
    args = parser.parse_args()

    setup_logging()
    download_year(args.year, force=args.force)


if __name__ == "__main__":
    main()