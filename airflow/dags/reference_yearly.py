"""Yearly pipeline: refresh the public holidays and the taxi zone lookup.

Holidays for year Y are published well before Y starts, so this runs on 1 December for the
year ahead. The zone lookup has no schedule of its own; refreshing it yearly is enough,
and the snapshot records any renames.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

DBT_DIR = "/opt/airflow/dbt/rain_and_rides"


@dag(
    dag_id="reference_yearly",
    schedule="0 5 1 12 *",                                   # 05:00 on 1 December
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "execution_timeout": pendulum.duration(minutes=30),
    },
    tags=["reference", "yearly"],
    doc_md=__doc__,
)
def reference_yearly():

    @task
    def target_year(logical_date=None) -> int:
        """The year this run is fetching: the December run prepares the year ahead."""
        return pendulum.instance(logical_date).year + 1

    @task
    def download_holidays(year: int) -> int:
        from ingestion.holidays import download_year

        download_year(year)
        return year

    @task
    def load_holidays(year: int) -> None:
        from spark_jobs.load_silver import load_holidays as load

        load(year)

    @task
    def download_zones() -> None:
        from ingestion.tlc import download_zone_lookup

        download_zone_lookup()

    @task.bash
    def dbt_snapshot_zones() -> str:
        """Record any zone renames before the seed is overwritten next time."""
        return f"cd {DBT_DIR} && dbt seed --select taxi_zone_lookup && dbt snapshot"

    year = target_year()
    load_holidays(download_holidays(year))
    download_zones() >> dbt_snapshot_zones()


reference_yearly()