"""Download hourly New York weather from the Open-Meteo archive into the bronze layer.

Run inside the Airflow container:
    python -m ingestion.weather --year 2025 --month 1

Files land in:
    data/bronze/weather/year=YYYY/month=MM/nyc_weather_YYYY-MM.json

Weather data by Open-Meteo.com (CC BY 4.0).
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from ingestion.common import DATA_DIR, TIMEOUT, atomic_output, get_session, setup_logging

API_URL = "https://archive-api.open-meteo.com/v1/archive"

NYC = {"latitude": 40.71, "longitude": -74.01}
HOURLY_VARS = [
    "temperature_2m",   # °C
    "precipitation",    # mm, rain + snow combined
    "rain",             # mm
    "snowfall",         # cm
    "wind_speed_10m",   # km/h
    "weather_code",     # WMO code: 0 = clear, 61 = light rain, 71 = light snow, ...
]
ARCHIVE_DELAY_DAYS = 5  # the archive lags a few days behind today

log = logging.getLogger(__name__)


class NotAvailableYetError(Exception):
    """The month is too recent to be complete in the archive."""


def month_range(year: int, month: int) -> tuple[date, date]:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def build_path(year: int, month: int) -> Path:
    return (
        DATA_DIR / "bronze" / "weather"
        / f"year={year}" / f"month={month:02d}"
        / f"nyc_weather_{year}-{month:02d}.json"
    )


def fetch_month(year: int, month: int) -> bytes:
    """Call the API for one month and return the raw response body."""
    start, end = month_range(year, month)
    if end > date.today() - timedelta(days=ARCHIVE_DELAY_DAYS):
        raise NotAvailableYetError(
            f"{year}-{month:02d} is too recent: the archive lags about {ARCHIVE_DELAY_DAYS} days behind"
        )

    params = {
        **NYC,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "America/New_York",  # same local time as the taxi timestamps
    }
    log.info("Requesting weather for %s to %s", start, end)
    resp = get_session().get(API_URL, params=params, timeout=TIMEOUT)
    if resp.status_code == 400:
        # Open-Meteo explains bad requests in a JSON "reason" field
        raise ValueError(f"Open-Meteo rejected the request: {resp.json().get('reason')}")
    resp.raise_for_status()
    return resp.content


def validate(payload: dict, year: int, month: int) -> None:
    """Refuse to save a response that is incomplete or not what we asked for."""
    hourly = payload.get("hourly") or {}
    missing = [field for field in ["time", *HOURLY_VARS] if field not in hourly]
    if missing:
        raise ValueError(f"Response is missing hourly fields: {missing}")

    hours = len(hourly["time"])
    expected = calendar.monthrange(year, month)[1] * 24
    if abs(hours - expected) > 1:  # allow 1 hour either way for daylight saving changes
        raise ValueError(f"Expected about {expected} hours, got {hours}")
    if any(len(hourly[field]) != hours for field in HOURLY_VARS):
        raise ValueError("Hourly fields have different lengths")
    if any(value is None for value in hourly["temperature_2m"]):
        raise ValueError("Some hours have no data; the month may not be complete in the archive yet")


def download_month(year: int, month: int, force: bool = False) -> Path:
    """Fetch, validate and save one month. Safe to run again."""
    dest = build_path(year, month)
    if dest.exists() and not force:
        # Only validated files ever get their final name, so an existing file is complete
        log.info("Skip: %s already downloaded", dest.name)
        return dest

    raw = fetch_month(year, month)
    payload = json.loads(raw)
    validate(payload, year, month)

    # Save the response exactly as it arrived (bronze = untouched)
    with atomic_output(dest) as tmp:
        tmp.write_bytes(raw)
    log.info("Saved %s (%d hours)", dest, len(payload["hourly"]["time"]))
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download one month of hourly NYC weather.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="1-12")
    parser.add_argument("--force", action="store_true", help="download again even if the file exists")
    args = parser.parse_args()

    setup_logging()
    try:
        download_month(args.year, args.month, args.force)
    except NotAvailableYetError as e:
        log.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()