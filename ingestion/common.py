"""Shared helpers for every ingestion client.

    DATA_DIR        where the data lake lives
    TIMEOUT         how long to wait for a server
    get_session()   an HTTP session that retries temporary failures with backoff
    atomic_output() write to a .part file and only rename it when everything succeeded
    setup_logging() one log format for all command-line runs
"""
from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
TIMEOUT = (10, 60)  # seconds: (connecting, waiting for data)

# Worth retrying: rate limits (429) and temporary server trouble (5xx).
# Not retried: 400/403/404, because asking again won't change the answer.
RETRY_STATUSES = (429, 500, 502, 503, 504)

log = logging.getLogger(__name__)


MAX_RETRIES = 5


class LoggingRetry(Retry):
    """urllib3's Retry, but it writes to the log every time a request fails."""

    def increment(self, method=None, url=None, response=None, error=None, *args, **kwargs):
        reason = f"HTTP {response.status}" if response is not None else type(error).__name__
        try:
            new_retry = super().increment(method, url, response, error, *args, **kwargs)
        except MaxRetryError:
            log.error("Request to %s failed (%s), giving up after %d retries", url, reason, MAX_RETRIES)
            raise
        log.warning("Request to %s failed (%s), retry %d of %d", url, reason, len(new_retry.history), MAX_RETRIES)
        return new_retry


@lru_cache(maxsize=1)
def get_session() -> requests.Session:
    """One shared session. Failed requests are retried up to 5 times,
    waiting longer each time (0s, 2s, 4s, 8s, 16s), or as long as the server asks."""
    retry = LoggingRetry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=("HEAD", "GET"),
        respect_retry_after_header=True,
        raise_on_status=False,  # after the last retry, hand back the response so we raise a clear HTTPError
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = "rain-and-rides/0.1 (data engineering learning project)"
    return session


@contextmanager
def atomic_output(dest: Path) -> Iterator[Path]:
    """Give the caller a temporary .part path. If the block finishes, rename it to dest.
    If anything fails (even Ctrl+C), delete the .part file. dest only ever appears complete."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    try:
        yield tmp
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, dest)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("urllib3").setLevel(logging.ERROR)  # our LoggingRetry already reports retries