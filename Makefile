-include .env
export

COMPOSE = docker compose

.PHONY: up down logs ps restart rebuild psql clean ingest-tlc ingest-zones

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