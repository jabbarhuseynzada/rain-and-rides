# Rain & Rides

Does the weather change how New York takes taxis?

A local, end-to-end data engineering project built with free tools:
Docker, PostgreSQL, Apache Airflow, PySpark and dbt.

## Status
Work in progress: infrastructure is running.

## Run it locally
1. Copy `.env.example` to `.env` and fill in the values
2. `make up`
3. Airflow UI: http://localhost:8080 · Warehouse: localhost:5432 · Metabase: http://localhost:3000
4. make ingest-tlc YEAR=2025 MONTH=1

## Data sources
- NYC TLC Trip Record Data
- Open-Meteo Historical Weather API (CC BY 4.0)
- Nager.Date public holidays