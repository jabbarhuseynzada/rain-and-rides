"""Monthly pipeline: download one month of taxi and weather data, clean it, load it, rebuild the models.

Each run handles exactly ONE month, worked out from the run's logical date, never from
"today". That is what makes a backfill identical to a normal run: same code, different date.

Which month does a run process?
    TLC publishes a month's trip file about two months late, so a run on 5 March 2026
    processes JANUARY 2026 (logical date minus PUBLISH_DELAY_MONTHS).
    To process a specific month by hand, trigger the DAG with a config of
        {"month": "2025-04"}
    which overrides the calculation.
"""
from __future__ import annotations

import logging
import pendulum
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException

# Trip files appear roughly two months after the month they cover.
PUBLISH_DELAY_MONTHS = 2
DBT_DIR = "/opt/airflow/dbt/rain_and_rides"

log = logging.getLogger(__name__)


@dag(
    dag_id="taxi_monthly",
    schedule="0 6 5 * *",                                    # 06:00 on the 5th of each month
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,                                           # backfills are run on purpose, not on unpause
    max_active_runs=1,                                       # months must not overlap: Spark needs the memory
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": pendulum.duration(hours=2),
    },
    tags=["taxi", "monthly"],
    doc_md=__doc__,
)
def taxi_monthly():

    @task
    def target_month(logical_date=None, **context) -> dict:
        """Which month is this run processing? Everything downstream uses this, not today's date."""
        override = (context.get("dag_run").conf or {}).get("month")
        if override:
            month = pendulum.from_format(override, "YYYY-MM")
            log.info("Processing %s (from the run config)", month.format("YYYY-MM"))
        else:
            month = pendulum.instance(logical_date).start_of("month").subtract(months=PUBLISH_DELAY_MONTHS)
            log.info(
                "Run date %s minus %d months -> processing %s",
                pendulum.instance(logical_date).format("YYYY-MM-DD"),
                PUBLISH_DELAY_MONTHS,
                month.format("YYYY-MM"),
            )
        return {"year": month.year, "month": month.month, "label": month.format("YYYY-MM")}

    @task
    def download_trips(period: dict) -> str:
        """Skip (not fail) when TLC has not published this month yet."""
        from ingestion.tlc import NotPublishedError, download_month

        try:
            return str(download_month("yellow", period["year"], period["month"]))
        except NotPublishedError as e:
            raise AirflowSkipException(f"TLC has not published this month yet: {e}")

    @task
    def download_weather(period: dict) -> str:
        from ingestion.weather import NotAvailableYetError, download_month

        try:
            return str(download_month(period["year"], period["month"]))
        except NotAvailableYetError as e:
            raise AirflowSkipException(f"Weather archive is not complete yet: {e}")

    @task
    def clean_trips(period: dict, downloaded: str) -> None:
        from spark_jobs.clean_trips import clean_month

        clean_month(period["year"], period["month"])

    @task
    def flatten_weather(period: dict, downloaded: str) -> None:
        from spark_jobs.flatten_weather import flatten_month

        flatten_month(period["year"], period["month"])

    @task
    def load_warehouse(period: dict) -> None:
        """Load trips and weather together, so the marts never see one without the other."""
        from spark_jobs.load_silver import load_month

        load_month(period["year"], period["month"])

    @task.bash
    def dbt_build(period: dict) -> str:
        """Rebuild the models and run every test. fct_trips only reprocesses this month."""
        return (
            f"cd {DBT_DIR} && "
            f"dbt build --vars '{{months: {period['label']}}}'"
        )

    period = target_month()
    trips_file = download_trips(period)
    weather_file = download_weather(period)

    cleaned = clean_trips(period, trips_file)
    flattened = flatten_weather(period, weather_file)

    loaded = load_warehouse(period)
    [cleaned, flattened] >> loaded >> dbt_build(period)


taxi_monthly()