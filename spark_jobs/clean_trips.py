"""Clean one month of yellow taxi trips: bronze -> silver, bad rows -> quarantine.

Run inside the Airflow container:
    python spark_jobs/clean_trips.py --year 2025 --month 1

Reads   data/bronze/tlc/yellow/year=YYYY/month=MM/
Writes  data/silver/trips/yellow/year=YYYY/month=MM/       rows that pass every rule
        data/quarantine/trips/yellow/year=YYYY/month=MM/   rows that fail one, with a reject_reason
Re-running a month replaces only that month's output, so the job is safe to run again.
"""
from __future__ import annotations

import argparse
import sys

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (DecimalType, DoubleType, IntegerType, StringType,
                               TimestampNTZType)

from spark_utils import DATA_DIR, get_spark

TAXI_TYPE = "yellow"

# ---------------------------------------------------------------- decisions from the exploration

QUARANTINE_ZERO_DISTANCE = False  # kept: normal duration + fare but 0.0 miles = a real ride with a broken odometer
MIN_DURATION_MIN = 1              # shorter = cancelled or accidental meter start
MAX_DURATION_MIN = 6 * 60         # longer = meter left running
MAX_DISTANCE_MILES = 100          # further = meter error (we saw 276,423 miles)
AIRPORT_ZONES = [1, 132, 138]     # Newark, JFK, LaGuardia in the taxi zone lookup
CREDIT_CARD = 1                   # data dictionary: tips are only recorded for credit card payments

# ---------------------------------------------------------------- the silver schema (the "contract")

MONEY = DecimalType(12, 2)  # exact cents: doubles give values like -1.9500000000000002

# silver column name, bronze column name, type
SCHEMA = [
    ("vendor_id",             "VendorID",              IntegerType()),
    ("pickup_datetime",       "tpep_pickup_datetime",  TimestampNTZType()),
    ("dropoff_datetime",      "tpep_dropoff_datetime", TimestampNTZType()),
    ("passenger_count",       "passenger_count",       IntegerType()),
    ("trip_distance",         "trip_distance",         DoubleType()),
    ("ratecode_id",           "RatecodeID",            IntegerType()),
    ("store_and_fwd_flag",    "store_and_fwd_flag",    StringType()),
    ("pu_location_id",        "PULocationID",          IntegerType()),
    ("do_location_id",        "DOLocationID",          IntegerType()),
    ("payment_type",          "payment_type",          IntegerType()),
    ("fare_amount",           "fare_amount",           MONEY),
    ("extra",                 "extra",                 MONEY),
    ("mta_tax",               "mta_tax",               MONEY),
    ("tip_amount",            "tip_amount",            MONEY),
    ("tolls_amount",          "tolls_amount",          MONEY),
    ("improvement_surcharge", "improvement_surcharge", MONEY),
    ("total_amount",          "total_amount",          MONEY),
    ("congestion_surcharge",  "congestion_surcharge",  MONEY),
    ("airport_fee",           "Airport_fee",           MONEY),
    ("cbd_congestion_fee",    "cbd_congestion_fee",    MONEY),
]

# Columns older files may not have, and the value to use instead
DEFAULTS_WHEN_MISSING = {
    # Congestion pricing started on 5 Jan 2025. Before that nobody paid it,
    # so 0.00 is the true value, not "unknown" (which would be null).
    "cbd_congestion_fee": 0,
}


def conform(raw: DataFrame) -> DataFrame:
    """Rename to snake_case, cast every column to its silver type, fill known-missing columns."""
    by_lower = {c.lower(): c for c in raw.columns}  # match names case-insensitively (Airport_fee vs airport_fee)
    columns = []
    for target, source, dtype in SCHEMA:
        actual = by_lower.get(source.lower())
        if actual is not None:
            columns.append(F.col(actual).cast(dtype).alias(target))
        elif target in DEFAULTS_WHEN_MISSING:
            columns.append(F.lit(DEFAULTS_WHEN_MISSING[target]).cast(dtype).alias(target))
        else:
            raise ValueError(f"Bronze data has no column '{source}' and no default is defined for it")

    unexpected = sorted(set(by_lower) - {source.lower() for _, source, _ in SCHEMA})
    if unexpected:
        print(f"WARNING: bronze has columns the silver schema doesn't know (ignored): {unexpected}")
    return raw.select(columns)


def add_derived(df: DataFrame) -> DataFrame:
    duration = F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / 60
    return (
        df
        .withColumn("passenger_count", F.when(F.col("passenger_count") > 0, F.col("passenger_count")))  # 0 -> null
        .withColumn("duration_min", F.round(duration, 2))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        # join key for hourly weather. date_trunc returns a timezone-aware timestamp, so cast it
        # back to "no timezone" like the other timestamps (safe: the session timezone is UTC)
        .withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime").cast(TimestampNTZType()))
        .withColumn(
            "tip_pct",  # only meaningful where tips are recorded, and never divide by zero
            F.when(
                (F.col("payment_type") == CREDIT_CARD) & (F.col("fare_amount") > 0),
                F.round(F.col("tip_amount").cast("double") / F.col("fare_amount").cast("double") * 100, 2),
            ),
        )
        .withColumn("is_airport_trip",
                    F.col("pu_location_id").isin(AIRPORT_ZONES) | F.col("do_location_id").isin(AIRPORT_ZONES))
    )


def reject_reason(year: int, month: int) -> Column:
    """The first rule a row breaks, or null if it passes them all. Order = priority."""
    rules = [
        ("missing_timestamp",     F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull()),
        ("pickup_outside_month",  (F.year("pickup_datetime") != year) | (F.month("pickup_datetime") != month)),
        ("dropoff_before_pickup", F.col("dropoff_datetime") < F.col("pickup_datetime")),
        ("too_short",             F.col("duration_min") < MIN_DURATION_MIN),
        ("too_long",              F.col("duration_min") > MAX_DURATION_MIN),
        ("negative_amount",       (F.col("fare_amount") < 0) | (F.col("total_amount") < 0)),
        ("too_far",               F.col("trip_distance") > MAX_DISTANCE_MILES),
    ]
    if QUARANTINE_ZERO_DISTANCE:
        rules.append(("zero_distance", F.col("trip_distance") == 0))

    reason = F.lit(None).cast("string")
    for name, condition in reversed(rules):  # build from the back so the first rule wins
        reason = F.when(condition, F.lit(name)).otherwise(reason)
    return reason


def month_dir(layer: str, year: int, month: int) -> str:
    folder = {"bronze": f"bronze/tlc/{TAXI_TYPE}", "silver": f"silver/trips/{TAXI_TYPE}",
              "quarantine": f"quarantine/trips/{TAXI_TYPE}"}[layer]
    return str(DATA_DIR / folder / f"year={year}" / f"month={month:02d}")


def clean_month(year: int, month: int) -> None:
    spark = get_spark(f"clean_trips_{year}-{month:02d}")
    bronze_path = month_dir("bronze", year, month)
    silver_path = month_dir("silver", year, month)
    quarantine_path = month_dir("quarantine", year, month)

    raw = spark.read.parquet(bronze_path)
    bronze_rows = raw.count()

    df = add_derived(conform(raw)).withColumn("reject_reason", reject_reason(year, month))
    df.persist(StorageLevel.MEMORY_AND_DISK)  # used by both writes below: read and transform bronze only once

    # mode("overwrite") on the month's own folder = idempotent: a re-run replaces this month and nothing else
    df.filter(F.col("reject_reason").isNull()).drop("reject_reason") \
      .write.mode("overwrite").parquet(silver_path)
    df.filter(F.col("reject_reason").isNotNull()) \
      .write.mode("overwrite").parquet(quarantine_path)
    df.unpersist()

    # Reconcile using what is actually on disk, not what we think we wrote
    silver_rows = spark.read.parquet(silver_path).count()
    quarantine = spark.read.parquet(quarantine_path)
    quarantine_rows = quarantine.count()
    by_reason = quarantine.groupBy("reject_reason").count().orderBy(F.desc("count")).collect()

    print(f"\n=== {TAXI_TYPE} {year}-{month:02d}")
    print(f"  bronze rows      {bronze_rows:>12,}")
    print(f"  silver rows      {silver_rows:>12,}  {silver_rows / bronze_rows:7.2%}")
    print(f"  quarantined rows {quarantine_rows:>12,}  {quarantine_rows / bronze_rows:7.2%}")
    for row in by_reason:
        print(f"    {row['reject_reason']:<24} {row['count']:>10,}")

    if bronze_rows != silver_rows + quarantine_rows:
        raise RuntimeError(
            f"Row counts don't reconcile: {bronze_rows} bronze != {silver_rows} silver + {quarantine_rows} quarantine"
        )
    print("  reconciled: bronze = silver + quarantine")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean one month of yellow taxi trips.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="1-12")
    args = parser.parse_args()
    try:
        clean_month(args.year, args.month)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()