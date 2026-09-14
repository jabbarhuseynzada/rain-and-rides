"""Look at the raw yellow taxi data before writing any cleaning code.

Part 1 (always): compare the schema of every downloaded month, to find schema drift.
Part 2 (--profile): profile one month, to find the bad rows the cleaning job must handle.

Run inside the Airflow container:
    python -m spark_jobs.explore_yellow
    python -m spark_jobs.explore_yellow --profile 2025-01
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.spark_utils import DATA_DIR, get_spark

YELLOW_DIR = DATA_DIR / "bronze" / "tlc" / "yellow"
PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"

# From the TLC data dictionary. Code 0 is deliberately left for you to look up.
PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}



# ---------------------------------------------------------------- part 1: schemas

def compare_schemas(spark: SparkSession) -> None:
    files = sorted(YELLOW_DIR.glob("year=*/month=*/*.parquet"))
    if not files:
        print(f"No files found in {YELLOW_DIR}. Run make ingest-tlc first.")
        return

    # Reading only the schema is cheap: Spark looks at the file footer, not the rows
    schemas: dict[str, dict[str, str]] = {}
    for f in files:
        period = f.stem.rsplit("_", 1)[-1]  # yellow_tripdata_2025-01 -> 2025-01
        schemas[period] = {field.name: field.dataType.simpleString() for field in spark.read.parquet(str(f)).schema}

    periods = list(schemas)
    columns = list(dict.fromkeys(c for s in schemas.values() for c in s))  # every column, first-seen order
    width = max(len(c) for c in columns) + 2

    print(f"\n=== Schemas of {len(periods)} yellow taxi files\n")
    header = "column".ljust(width) + "".join(p.ljust(15) for p in periods)
    print(header)
    print("-" * len(header))
    drifted = []
    for col in columns:
        types = [schemas[p].get(col, "MISSING") for p in periods]
        differs = len(set(types)) > 1
        if differs:
            drifted.append(col)
        print(col.ljust(width) + "".join(t.ljust(15) for t in types) + ("  <-- differs" if differs else ""))

    # Same column, different capitalisation (e.g. Airport_fee vs airport_fee) is drift too
    spellings = defaultdict(set)
    for col in columns:
        spellings[col.lower()].add(col)
    renamed = [sorted(names) for names in spellings.values() if len(names) > 1]

    print(f"\nColumns that differ between files: {drifted or 'none'}")
    print(f"Same column spelled differently:   {renamed or 'none'}")


# ---------------------------------------------------------------- part 2: one month in detail

def load_month(spark: SparkSession, period: str) -> DataFrame:
    year, month = period.split("-")
    path = YELLOW_DIR / f"year={year}" / f"month={month}" / f"yellow_tripdata_{period}.parquet"
    return spark.read.parquet(str(path))


def profile_month(spark: SparkSession, period: str) -> None:
    year, month = map(int, period.split("-"))
    df = load_month(spark, period).withColumn(
        "duration_min", F.expr(f"timestampdiff(SECOND, {PICKUP}, {DROPOFF})") / 60
    )
    df.cache()  # we scan this month several times below; keep it in memory after the first scan
    total = df.count()
    print(f"\n=== Profile of {period}: {total:,} rows\n")

    print("Three example rows:")
    df.show(3, vertical=True, truncate=False)

    print("Distribution of the main numbers:")
    df.select("trip_distance", "fare_amount", "tip_amount", "total_amount", "passenger_count", "duration_min") \
      .summary("min", "1%", "25%", "50%", "75%", "99%", "max").show(truncate=False)

    # Every check becomes one column in a single select, so Spark scans the data once, not ten times
    checks = {
        "pickup outside the month":          (F.year(PICKUP) != year) | (F.month(PICKUP) != month),
        "dropoff before pickup":             F.col(DROPOFF) < F.col(PICKUP),
        "trip under 1 minute":               F.col("duration_min") < 1,
        "trip over 6 hours":                 F.col("duration_min") > 360,
        "negative fare":                     F.col("fare_amount") < 0,
        "negative total":                    F.col("total_amount") < 0,
        "zero distance":                     F.col("trip_distance") == 0,
        "distance over 100 miles":           F.col("trip_distance") > 100,
        "passenger_count missing":           F.col("passenger_count").isNull(),
        "pickup zone 264/265 (unknown/N/A)": F.col("PULocationID").isin(264, 265),
    }
    counts = df.select([F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in checks.items()]).first()

    print("Suspicious rows (a row can appear in several checks):")
    for name in checks:
        n = counts[name] or 0
        print(f"  {name:<36} {n:>12,}  {n / total:7.2%}")

    print("\nMissing values per column (only columns with any):")
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first()
    for c in df.columns:
        if nulls[c]:
            print(f"  {c:<36} {nulls[c]:>12,}  {nulls[c] / total:7.2%}")

    # Who are the suspicious rows? Break three groups down by payment_type, in one pass
    groups = {
        "all rows":        F.lit(True),
        "passenger null":  F.col("passenger_count").isNull(),
        "negative fare":   F.col("fare_amount") < 0,
    }
    breakdown = (
        df.groupBy("payment_type")
          .agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in groups.items()])
          .orderBy("payment_type")
          .collect()
    )
    print("\nPayment type breakdown (codes from the TLC data dictionary):")
    print(f"  {'payment_type':<30}" + "".join(f"{name:>16}" for name in groups))
    for row in breakdown:
        code = row["payment_type"]
        label = f"{code} {PAYMENT_TYPES.get(code, '(look this up)')}"
        print(f"  {label:<30}" + "".join(f"{row[name]:>16,}" for name in groups))

    df.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Explore raw yellow taxi files.")
    parser.add_argument("--profile", metavar="YYYY-MM", help="also profile one month in detail")
    args = parser.parse_args()

    spark = get_spark("explore_yellow")
    try:
        compare_schemas(spark)
        if args.profile:
            profile_month(spark, args.profile)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()