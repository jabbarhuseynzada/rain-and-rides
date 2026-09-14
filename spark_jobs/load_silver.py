"""Load silver data into the Postgres warehouse (raw schema).

Run inside the Airflow container:
    python -m spark_jobs.load_silver --year 2025 --month 1     # trips + weather for one month
    python -m spark_jobs.load_silver --year 2025 --month 1 --only weather
    python -m spark_jobs.load_silver --year 2025 --holidays    # holidays for one year

How a load works (the "staging table" pattern):
    1. Spark writes the batch into a staging table, e.g. raw.yellow_trips_staging (replaced every time)
    2. In ONE Postgres transaction: delete that batch from the real table, copy the staging rows in
    3. If anything fails, the transaction rolls back and the real table is untouched
Re-running a month replaces that month and nothing else.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.db import connect
from spark_jobs.spark_utils import DATA_DIR, get_spark


def jdbc_options() -> tuple[str, dict]:
    host = os.environ.get("WAREHOUSE_HOST", "postgres")
    port = os.environ.get("WAREHOUSE_PORT", "5432")
    db = os.environ["WAREHOUSE_DB"]
    url = f"jdbc:postgresql://{host}:{port}/{db}?reWriteBatchedInserts=true"  # much faster batch inserts
    props = {
        "user": os.environ["WAREHOUSE_USER"],
        "password": os.environ["WAREHOUSE_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    return url, props


def load(df: DataFrame, target: str, batch: dict[str, int], label: str) -> None:
    """Replace one batch (e.g. year=2025, month=1) of target with the rows in df."""
    staging = f"{target}_staging"
    for key, value in batch.items():
        df = df.withColumn(key, F.lit(value))
    columns = ", ".join(df.columns)
    expected = df.count()

    # 1. Spark -> staging table. truncate=true empties it instead of dropping it; 4 parallel writers.
    url, props = jdbc_options()
    (df.repartition(4).write
       .mode("overwrite").option("truncate", "true").option("batchsize", 10000)
       .jdbc(url, staging, properties=props))

    # 2. Staging -> real table in one transaction. Table names come from this code (never from users),
    #    so formatting them in is safe; the batch values still go in as parameters.
    where = " AND ".join(f"{key} = %s" for key in batch)
    params = tuple(batch.values())
    with connect() as conn:  # commits at the end of the block, rolls back on any error
        deleted = conn.execute(f"DELETE FROM {target} WHERE {where}", params).rowcount
        conn.execute(f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {staging}")
        loaded = conn.execute(f"SELECT count(*) FROM {target} WHERE {where}", params).fetchone()[0]
        conn.execute(f"TRUNCATE {staging}")  # free the space; the rows now live in the real table
        if loaded != expected:
            raise RuntimeError(f"{target}: expected {expected} rows for {batch}, found {loaded}. Rolled back.")

    print(f"  {label:<10} {target:<20} {loaded:>12,} rows loaded  (replaced {deleted:,})")


def load_month(year: int, month: int, only: str | None = None) -> None:
    spark = get_spark(f"load_silver_{year}-{month:02d}")
    part = f"year={year}/month={month:02d}"
    print(f"\n=== Loading {year}-{month:02d} into Postgres")
    if only in (None, "trips"):
        trips = spark.read.parquet(str(DATA_DIR / "silver" / "trips" / "yellow" / part))
        load(trips, "raw.yellow_trips", {"year": year, "month": month}, "trips")
    if only in (None, "weather"):
        weather = spark.read.parquet(str(DATA_DIR / "silver" / "weather" / part))
        load(weather, "raw.weather_hourly", {"year": year, "month": month}, "weather")
    spark.stop()


def load_holidays(year: int) -> None:
    spark = get_spark(f"load_holidays_{year}")
    raw = spark.read.option("multiLine", True).json(str(DATA_DIR / "bronze" / "holidays" / f"year={year}"))
    holidays = raw.select(
        F.to_date("date").alias("holiday_date"),
        F.col("name"),
        F.col("localName").alias("local_name"),
        F.col("global").alias("is_global"),
        F.when(F.col("counties").isNotNull(), F.concat_ws(",", "counties")).alias("counties"),
        F.when(F.col("types").isNotNull(), F.concat_ws(",", "types")).alias("types"),
    )
    print(f"\n=== Loading {year} holidays into Postgres")
    load(holidays, "raw.holidays", {"year": year}, "holidays")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load silver data into the Postgres raw schema.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), metavar="1-12")
    parser.add_argument("--holidays", action="store_true", help="load that year's holidays instead")
    parser.add_argument("--only", choices=["trips", "weather"], help="load just one of the two datasets")
    args = parser.parse_args()

    if args.holidays:
        load_holidays(args.year)
    elif args.month:
        load_month(args.year, args.month, args.only)
    else:
        parser.error("give --month (trips + weather) or --holidays")


if __name__ == "__main__":
    main()