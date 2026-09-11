"""Flatten one month of Open-Meteo weather: bronze JSON -> silver, one row per hour.

Run inside the Airflow container:
    python spark_jobs/flatten_weather.py --year 2025 --month 1

Reads   data/bronze/weather/year=YYYY/month=MM/*.json   hourly values in UTC
Writes  data/silver/weather/year=YYYY/month=MM/          one row per local New York hour

The bronze file is in UTC and covers one extra day, so we can convert every hour to New York time
with real daylight saving rules and then keep exactly the hours that fall inside the local month.
"""
from __future__ import annotations

import argparse
import calendar

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampNTZType

from spark_utils import DATA_DIR, get_spark

SOURCE_TIMEZONE = "GMT"             # what ingestion/weather.py requests
LOCAL_TIMEZONE = "America/New_York"  # what the taxi timestamps use

# Open-Meteo field -> silver column (units in the name, so nobody has to guess), type
HOURLY_FIELDS = {
    "temperature_2m": ("temperature_c",    DoubleType()),
    "precipitation":  ("precipitation_mm", DoubleType()),
    "rain":           ("rain_mm",          DoubleType()),
    "snowfall":       ("snowfall_cm",      DoubleType()),
    "wind_speed_10m": ("wind_speed_kmh",   DoubleType()),
    "weather_code":   ("weather_code",     IntegerType()),
}


def flatten_month(year: int, month: int) -> None:
    spark = get_spark(f"flatten_weather_{year}-{month:02d}")
    part = f"year={year}/month={month:02d}"
    bronze_path = str(DATA_DIR / "bronze" / "weather" / part)
    silver_path = str(DATA_DIR / "silver" / "weather" / part)

    raw = spark.read.option("multiLine", True).json(bronze_path)

    # Contract check: the file must be in UTC, or the conversion below would be wrong
    timezone = raw.select("timezone").first()["timezone"]
    if timezone != SOURCE_TIMEZONE:
        raise ValueError(f"Weather is in {timezone}, expected {SOURCE_TIMEZONE}. Re-download with --force.")

    # The JSON holds parallel arrays: hourly.time[i] belongs with hourly.temperature_2m[i], and so on.
    # arrays_zip pairs them up element by element; explode turns each pair into its own row.
    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        *[F.col(f"hourly.{field}").alias(field) for field in HOURLY_FIELDS],
    )
    utc_hour = F.to_timestamp(F.col("h.time"), "yyyy-MM-dd'T'HH:mm")  # parsed as UTC (session timezone)
    # from_utc_timestamp applies New York's real rules: UTC-5 in winter, UTC-4 in summer
    local_hour = F.from_utc_timestamp(utc_hour, LOCAL_TIMEZONE).cast(TimestampNTZType())
    hours = (
        raw.select(F.explode(zipped).alias("h"))
        .select(
            local_hour.alias("weather_hour"),
            *[F.col(f"h.{field}").cast(dtype).alias(name) for field, (name, dtype) in HOURLY_FIELDS.items()],
        )
        # the file covers an extra UTC day; keep only hours inside this local month
        .filter((F.year("weather_hour") == year) & (F.month("weather_hour") == month))
    )
    hours.cache()
    raw_hours = hours.count()

    # When daylight saving ends in November, the 01:00 local hour happens twice. Two rows with the
    # same weather_hour would double every taxi trip in the join, so merge them: average the
    # temperature and wind, add up precipitation, keep the most severe weather code.
    merged = (
        hours.groupBy("weather_hour")
        .agg(
            F.round(F.avg("temperature_c"), 1).alias("temperature_c"),
            F.round(F.sum("precipitation_mm"), 1).alias("precipitation_mm"),
            F.round(F.sum("rain_mm"), 1).alias("rain_mm"),
            F.round(F.sum("snowfall_cm"), 2).alias("snowfall_cm"),
            F.round(F.avg("wind_speed_kmh"), 1).alias("wind_speed_kmh"),
            F.max("weather_code").alias("weather_code"),
        )
        .withColumn("weather_date", F.to_date("weather_hour"))
        .orderBy("weather_hour")
    )

    # 744 rows is tiny: one file instead of Spark's usual several (too many small files slow everything down)
    merged.coalesce(1).write.mode("overwrite").parquet(silver_path)
    hours.unpersist()

    # Reconcile using what is on disk
    silver = spark.read.parquet(silver_path)
    silver_hours = silver.count()
    null_temps = silver.filter(F.col("temperature_c").isNull()).count()
    expected = calendar.monthrange(year, month)[1] * 24

    print(f"\n=== weather {year}-{month:02d}")
    print(f"  local hours in month   {raw_hours:>6}")
    print(f"  duplicate hours merged {raw_hours - silver_hours:>6}")
    print(f"  hours in silver        {silver_hours:>6}   (a {calendar.month_name[month]} has {expected})")
    print(f"  hours without temp     {null_temps:>6}")

    if abs(silver_hours - expected) > 1:  # daylight saving can remove or merge one hour
        raise RuntimeError(f"Expected about {expected} hours, got {silver_hours}")
    if null_temps:
        raise RuntimeError(f"{null_temps} hours have no temperature")
    print("  checks passed")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Flatten one month of hourly weather into silver.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="1-12")
    args = parser.parse_args()
    flatten_month(args.year, args.month)


if __name__ == "__main__":
    main()