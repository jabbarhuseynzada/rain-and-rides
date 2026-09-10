"""Download NYC TLC trip record files into the bronze layer.

Run inside the Airflow container:
    python -m ingestion.tlc --year 2025 --month 1

Files land in:
    data/bronze/tlc/<taxi_type>/year=YYYY/month=MM/<taxi_type>_tripdata_YYYY-MM.parquet
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
CHUNK_SIZE = 1024 * 1024  # write the download to disk 1 MB at a time
TIMEOUT = (10, 60)        # seconds: (connecting, waiting for data)

log = logging.getLogger(__name__)


class NotPublishedError(Exception):
    """The requested month is not on the TLC site (yet)."""


def build_url(taxi_type: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def build_path(taxi_type: str, year: int, month: int) -> Path:
    return (
        DATA_DIR / "bronze" / "tlc" / taxi_type
        / f"year={year}" / f"month={month:02d}"
        / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    )


def remote_size(url: str) -> int | None:
    """Ask the server how big the file is, without downloading it."""
    resp = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
    if resp.status_code in (403, 404):
        raise NotPublishedError(f"Not available (HTTP {resp.status_code}): {url}")
    resp.raise_for_status()
    size = resp.headers.get("Content-Length")
    return int(size) if size else None


def is_valid_parquet(path: Path) -> bool:
    """Every Parquet file starts and ends with the 4 bytes b'PAR1'."""
    if path.stat().st_size < 12:
        return False
    with path.open("rb") as f:
        head = f.read(4)
        f.seek(-4, os.SEEK_END)
        tail = f.read(4)
    return head == tail == b"PAR1"


def download_month(taxi_type: str, year: int, month: int, force: bool = False) -> Path:
    """Download one month. Safe to run again: a complete file is never downloaded twice."""
    url = build_url(taxi_type, year, month)
    dest = build_path(taxi_type, year, month)
    expected = remote_size(url)

    # Idempotency: skip if we already have the complete file
    if dest.exists() and not force:
        if expected is None or dest.stat().st_size == expected:
            log.info("Skip: %s already downloaded (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
            return dest
        log.warning("Size mismatch for %s, downloading again", dest.name)

    # Download to a temporary .part file first...
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.part")
    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)

    # ...check it...
    if expected is not None and tmp.stat().st_size != expected:
        tmp.unlink()
        raise IOError(f"Incomplete download for {dest.name}: expected {expected} bytes")
    if not is_valid_parquet(tmp):
        tmp.unlink()
        raise IOError(f"{dest.name} is not a valid Parquet file")

    # ...and only then give it its real name. A half-finished file can never look complete.
    os.replace(tmp, dest)
    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download one month of NYC TLC trip data.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="1-12")
    parser.add_argument("--taxi-type", default="yellow", choices=["yellow", "green", "fhv", "fhvhv"])
    parser.add_argument("--force", action="store_true", help="download again even if the file exists")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        download_month(args.taxi_type, args.year, args.month, args.force)
    except NotPublishedError as e:
        log.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()