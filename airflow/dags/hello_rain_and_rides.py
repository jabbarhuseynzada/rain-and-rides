"""A tiny DAG to prove Airflow works and can see the data lake."""
import os
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,              # only runs when you trigger it manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
)
def hello_rain_and_rides():
    @task
    def say_hello():
        print("Airflow is running!")

    @task
    def check_data_lake():
        path = "/opt/airflow/data"
        print(f"Folders in {path}: {os.listdir(path)}")

    say_hello() >> check_data_lake()


hello_rain_and_rides()