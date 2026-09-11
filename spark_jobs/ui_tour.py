"""A small job to watch in the Spark UI (http://localhost:4040).

Run inside the Airflow container:
    python spark_jobs/ui_tour.py

It runs four queries on your silver trips, then keeps Spark alive until you press Enter,
because the Spark UI only exists while a Spark application is running.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from spark_utils import DATA_DIR, get_spark

TRIPS = str(DATA_DIR / "silver" / "trips" / "yellow")


def main() -> None:
    spark = get_spark("spark_ui_tour")
    sc = spark.sparkContext
    trips = spark.read.parquet(TRIPS)  # all months; year and month come from the folder names

    # 1. Each partition counts its own rows; only those few numbers are shuffled to one task to add up.
    sc.setJobDescription("1 count: tiny shuffle")
    print(f"\n1. Airport trips in all months: {trips.filter('is_airport_trip').count():,}")

    # 2. Rows of one zone must meet in one place: a shuffle. But each partition first shrinks its
    #    millions of rows to ~260 zone subtotals ("partial aggregation"), so the shuffle stays small.
    sc.setJobDescription("2 group by zone: small shuffle")
    print("\n2. Busiest pickup zones:")
    (trips.groupBy("pu_location_id")
          .agg(F.count("*").alias("trips"), F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct"))
          .orderBy(F.desc("trips"))
          .show(5))

    # 3. Almost every pickup time is unique, so there is nothing to shrink first:
    #    millions of values cross the shuffle. Compare its Shuffle Write size with query 2.
    sc.setJobDescription("3 distinct pickup times: big shuffle")
    print(f"\n3. Distinct pickup times: {trips.select('pickup_datetime').distinct().count():,}")

    # 4. Filtering on the folder columns lets Spark skip the other months' folders entirely.
    sc.setJobDescription("4 partition pruning: only 2025-01")
    print(f"\n4. Trips in January 2025: {trips.filter('year = 2025 AND month = 1').count():,}")

    input("\nThe Spark UI is open at http://localhost:4040\nExplore it, then press Enter here to stop Spark... ")
    spark.stop()


if __name__ == "__main__":
    main()