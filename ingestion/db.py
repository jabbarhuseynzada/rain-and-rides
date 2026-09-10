"""Talk to the warehouse. For now: record every ingestion run in raw.ingestion_log.

Connection settings come from WAREHOUSE_* environment variables (set in docker-compose.yml).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import psycopg

from ingestion.common import DATA_DIR

log = logging.getLogger(__name__)


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("WAREHOUSE_HOST", "postgres"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
        connect_timeout=10,
    )


def record_run(source: str, period: str | None, path: Path, status: str, row_count: int | None) -> None:
    """Insert one row into raw.ingestion_log."""
    with connect() as conn:  # commits when the block ends without an error
        conn.execute(
            """
            INSERT INTO raw.ingestion_log (source, period, file_path, row_count, file_bytes, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (source, period, str(path.relative_to(DATA_DIR)), row_count, path.stat().st_size, status),
        )
    label = f"{source} {period}" if period else source
    log.info("Logged run: %s %s (%s rows)", label, status, row_count)   