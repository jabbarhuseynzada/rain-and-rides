"""Download NYC TLC files into the bronze layer.

Run inside the Airflow container:
    python -m ingestion.tlc --year 2025 --month 1     # one month of trips
    python -m ingestion.tlc --zones                   # taxi zone lookup table

Files land in:
    data/bronze/tlc/<taxi_type>/year=YYYY/month=MM/<taxi_type>_tripdata_YYYY-MM.parquet
    data/bronze/tlc/reference/taxi_zone_lookup.csv
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.common import DATA_DIR, TIMEOUT, atomic_output, get_session, setup_logging
from ingestion.db import record_run

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
CHUNK_SIZE = 1024 * 1024  # write the download to disk 1 MB at a time

log = logging.getLogger(__name__)


class NotPublishedError(Exception):
    """The requested file is not on the TLC site (yet)."""


# ---------------------------------------------------------------- paths and URLs

def build_url(taxi_type: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def build_path(taxi_type: str, year: int, month: int) -> Path:
    return (
        DATA_DIR / "bronze" / "tlc" / taxi_type
        / f"year={year}" / f"month={month:02d}"
        / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    )


def zones_path() -> Path:
    return DATA_DIR / "bronze" / "tlc" / "reference" / "taxi_zone_lookup.csv"


# ---------------------------------------------------------------- validators

def is_valid_parquet(path: Path) -> bool:
    """Every Parquet file starts and ends with the 4 bytes b'PAR1'."""
    if path.stat().st_size < 12:
        return False
    with path.open("rb") as f:
        head = f.read(4)
        f.seek(-4, os.SEEK_END)
        tail = f.read(4)
    return head == tail == b"PAR1"


def is_valid_zone_csv(path: Path) -> bool:
    """The zone lookup's header row must contain the LocationID column."""
    with path.open("rb") as f:
        header = f.readline()
    return b"LocationID" in header


# ---------------------------------------------------------------- row counts (for the ingestion log)

def parquet_row_count(path: Path) -> int:
    """Read the row count from the Parquet footer, without loading the data."""
    return pq.read_metadata(path).num_rows


def csv_row_count(path: Path) -> int:
    with path.open("rb") as f:
        return sum(1 for _ in f) - 1  # minus the header row


# ---------------------------------------------------------------- shared download logic

def remote_size(url: str) -> int | None:
    """Ask the server how big the file is, without downloading it."""
    resp = get_session().head(url, timeout=TIMEOUT, allow_redirects=True)
    if resp.status_code in (403, 404):
        raise NotPublishedError(f"Not available (HTTP {resp.status_code}): {url}")
    resp.raise_for_status()
    size = resp.headers.get("Content-Length")
    return int(size) if size else None


def download_file(url: str, dest: Path, validate: Callable[[Path], bool], force: bool = False) -> tuple[Path, str]:
    """Download url to dest. Safe to run again: a complete file is never downloaded twice.
    Returns the path and a status: "downloaded" or "skipped"."""
    expected = remote_size(url)

    # Idempotency: skip if we already have the complete file
    if dest.exists() and not force:
        if expected is None or dest.stat().st_size == expected:
            log.info("Skip: %s already downloaded (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
            return dest, "skipped"
        log.warning("Size mismatch for %s, downloading again", dest.name)

    log.info("Downloading %s", url)
    with atomic_output(dest) as tmp:
        with get_session().get(url, stream=True, timeout=TIMEOUT) as resp:
            resp.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)

        # Any error raised here deletes the .part file, so dest never appears
        if expected is not None and tmp.stat().st_size != expected:
            raise IOError(f"Incomplete download for {dest.name}: expected {expected} bytes")
        if not validate(tmp):
            raise IOError(f"{dest.name} failed validation ({validate.__name__})")

    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
    return dest, "downloaded"


# ---------------------------------------------------------------- public functions (Airflow will call these)

def download_month(taxi_type: str, year: int, month: int, force: bool = False) -> Path:
    path, status = download_file(build_url(taxi_type, year, month), build_path(taxi_type, year, month),
                                 is_valid_parquet, force)
    record_run(f"tlc_{taxi_type}", f"{year}-{month:02d}", path, status, parquet_row_count(path))
    return path


def download_zone_lookup(force: bool = False) -> Path:
    path, status = download_file(ZONES_URL, zones_path(), is_valid_zone_csv, force)
    record_run("tlc_zones", None, path, status, csv_row_count(path))
    return path


# ---------------------------------------------------------------- command line

def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC trip data or the zone lookup table.")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int, choices=range(1, 13), metavar="1-12")
    parser.add_argument("--taxi-type", default="yellow", choices=["yellow", "green", "fhv", "fhvhv"])
    parser.add_argument("--zones", action="store_true", help="download the taxi zone lookup table instead")
    parser.add_argument("--force", action="store_true", help="download again even if the file exists")
    args = parser.parse_args()

    if not args.zones and (args.year is None or args.month is None):
        parser.error("--year and --month are required (or use --zones)")

    setup_logging()
    try:
        if args.zones:
            download_zone_lookup(args.force)
        else:
            download_month(args.taxi_type, args.year, args.month, args.force)
    except NotPublishedError as e:
        log.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()