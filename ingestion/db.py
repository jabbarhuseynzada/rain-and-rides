"""Talk to the warehouse. For now: record every ingestion run in raw.ingestion_log.

Where the credentials come from:
  1. the Airflow Connection "warehouse", when Airflow can resolve it (inside a task, or from
     the AIRFLOW_CONN_WAREHOUSE variable set in docker-compose.yml)
  2. otherwise the WAREHOUSE_* environment variables

Airflow Connections are the right place for credentials: they are stored encrypted in
Airflow's own database, can be changed in the UI without touching code, and never appear in
a DAG file. The environment fallback keeps the scripts runnable on their own.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path

import psycopg

from ingestion.common import DATA_DIR

WAREHOUSE_CONN_ID = "warehouse"

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def warehouse_settings() -> dict:
    """Host, port, database, user and password for the warehouse."""
    try:
        from airflow.sdk import BaseHook

        conn = BaseHook.get_connection(WAREHOUSE_CONN_ID)
        log.debug("Using the Airflow connection %s", WAREHOUSE_CONN_ID)
        return {
            "host": conn.host,
            "port": conn.port or 5432,
            "dbname": conn.schema,
            "user": conn.login,
            "password": conn.password,
        }
    except Exception:
        # Airflow is not available, or the connection is not defined: use the environment
        return {
            "host": os.environ.get("WAREHOUSE_HOST", "postgres"),
            "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
            "dbname": os.environ["WAREHOUSE_DB"],
            "user": os.environ["WAREHOUSE_USER"],
            "password": os.environ["WAREHOUSE_PASSWORD"],
        }


def connect() -> psycopg.Connection:
    return psycopg.connect(connect_timeout=10, **warehouse_settings())


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