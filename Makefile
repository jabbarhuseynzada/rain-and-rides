-include .env
export

COMPOSE = docker compose

.PHONY: up down logs ps restart rebuild psql clean ingest-tlc ingest-zones ingest-weather ingest-holidays explore-yellow clean-trips flatten-weather load-silver load-holidays spark-ui-tour dbt

up:
	$(COMPOSE) up -d

# Stop and remove containers and networks (database volume is kept)
down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d

rebuild:
	$(COMPOSE) up -d --build

# Open a SQL shell inside the warehouse
psql:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

# Deletes the database volume too, so all loaded data is lost
clean:
	$(COMPOSE) down -v

# Download one month of taxi data:  make ingest-tlc YEAR=2025 MONTH=1
ingest-tlc:
	$(COMPOSE) exec airflow-scheduler python -m ingestion.tlc --year $(YEAR) --month $(MONTH)

# Download the taxi zone lookup table
ingest-zones:
	$(COMPOSE) exec airflow-scheduler python -m ingestion.tlc --zones	

# Download one month of weather:  make ingest-weather YEAR=2025 MONTH=1
ingest-weather:
	$(COMPOSE) exec airflow-scheduler python -m ingestion.weather --year $(YEAR) --month $(MONTH)

# Download one year of holidays:  make ingest-holidays YEAR=2025
ingest-holidays:
	$(COMPOSE) exec airflow-scheduler python -m ingestion.holidays --year $(YEAR)

# Explore raw taxi data:  make explore-yellow   or   make explore-yellow PERIOD=2025-01
explore-yellow:
	$(COMPOSE) exec airflow-scheduler python spark_jobs/explore_yellow.py $(if $(PERIOD),--profile $(PERIOD))

# Clean one month of trips:  make clean-trips YEAR=2025 MONTH=1
clean-trips:
	$(COMPOSE) exec airflow-scheduler python spark_jobs/clean_trips.py --year $(YEAR) --month $(MONTH)

# Flatten one month of weather:  make flatten-weather YEAR=2025 MONTH=1
flatten-weather:
	$(COMPOSE) exec airflow-scheduler python spark_jobs/flatten_weather.py --year $(YEAR) --month $(MONTH)

# Load one month of silver into Postgres:  make load-silver YEAR=2025 MONTH=1
load-silver:
	$(COMPOSE) exec airflow-scheduler python spark_jobs/load_silver.py --year $(YEAR) --month $(MONTH)

# Load one year of holidays into Postgres:  make load-holidays YEAR=2025
load-holidays:
	$(COMPOSE) exec airflow-scheduler python spark_jobs/load_silver.py --year $(YEAR) --holidays

# Run a Spark job and keep the UI open:  make spark-ui-tour
spark-ui-tour:
	$(COMPOSE) exec airflow-scheduler python spark_jobs/ui_tour.py

# Run any dbt command:  make dbt CMD="debug"   make dbt CMD="build"
dbt:
	$(COMPOSE) exec airflow-scheduler dbt $(CMD)