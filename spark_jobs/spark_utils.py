"""Shared settings for every Spark job in this project."""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
JDBC_JAR = Path(os.environ.get("POSTGRES_JDBC_JAR", "/opt/spark-jars/postgresql.jar"))  # baked into the image


def get_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder
    if JDBC_JAR.exists():
        builder = builder.config("spark.jars", str(JDBC_JAR))  # lets Spark talk to Postgres
    spark = (
        builder
        .appName(app_name)
        .master("local[*]")                               # use every CPU core in this container
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")      # never silently shift timestamps
        .config("spark.sql.shuffle.partitions", "8")      # the default 200 is sized for clusters, not a laptop
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # hide Spark's own INFO/WARN chatter
    return spark