# Rain & Rides

Does the weather change how New York takes taxis?

A local, end-to-end data engineering project built with free tools:
Docker, PostgreSQL, Apache Airflow, PySpark and dbt.

## Status
Work in progress: infrastructure is running, raw data ingestion is being built.

## Run it locally
1. Copy `.env.example` to `.env` and fill in the values
2. `make up`
3. Airflow UI: http://localhost:8080 · Warehouse: localhost:5432 · Metabase: http://localhost:3000
4. Load some data with the ingestion commands below

## Ingestion commands

| Command | What it does |
|---|---|
| `make ingest-tlc YEAR=2025 MONTH=1` | Monthly taxi trips |
| `make ingest-zones` | Taxi zone lookup |
| `make ingest-weather YEAR=2025 MONTH=1` | Hourly NYC weather |
| `make ingest-holidays YEAR=2025` | Yearly US holidays |

Files land in `data/bronze/`. Running a command again skips files that are already downloaded.

## Data sources
- NYC TLC Trip Record Data
- Open-Meteo Historical Weather API (CC BY 4.0)
- Nager.Date public holidays