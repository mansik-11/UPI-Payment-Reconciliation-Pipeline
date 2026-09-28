# UPI Payment Reconciliation Pipeline Makefile
-include .env
export

DATE ?= $(shell date +%Y-%m-%d)
START ?= 2026-09-01
END ?= 2026-09-10

.PHONY: help setup lock docker-up docker-down init-db generate run backfill test dbt-snapshot dbt-run dbt-test dbt-seed clean

help:
	@echo "Available commands:"
	@echo "  make setup          - Install pinned dependencies (requirements.txt)"
	@echo "  make lock           - Re-pin requirements.txt from requirements.in"
	@echo "  make docker-up      - Start local Postgres via docker compose"
	@echo "  make docker-down    - Stop local Postgres"
	@echo "  make init-db        - Initialize Postgres schemas and tables"
	@echo "  make generate       - Generate 10 days of synthetic data"
	@echo "  make run DATE=...   - Ingest and process a specific date"
	@echo "  make backfill       - Run ingestion across a date range"
	@echo "  make test           - Run unit tests with pytest"
	@echo "  make dbt-seed       - Load dbt seeds (answer_key)"
	@echo "  make dbt-run        - Execute dbt models"
	@echo "  make dbt-test       - Execute dbt tests"

setup:
	pip install -r requirements.txt

lock:
	pip install -q pip-tools
	pip-compile requirements.in -o requirements.txt --resolver=backtracking --strip-extras

docker-up:
	docker compose up -d
	@echo "Waiting for Postgres to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' upi_postgres 2>/dev/null)" = "healthy" ]; do sleep 1; done

docker-down:
	docker compose down

init-db:
	PGPASSWORD="$(PG_PASSWORD)" psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/init.sql

generate:
	python generator/generate_data.py --days 10 --orders-per-day 20000

run:
	python ingestion/extract.py --run-date $(DATE)
	python ingestion/validate.py --run-date $(DATE)
	python ingestion/load.py --run-date $(DATE)
	cd dbt_upi && dbt seed --profiles-dir . && dbt snapshot --profiles-dir . && dbt run --profiles-dir . --vars '{"run_date": "$(DATE)"}' && dbt test --profiles-dir .

backfill:
	@d="$(START)"; \
	end="$(END)"; \
	while [ "$$(date -d "$$d" +%Y%m%d)" -le "$$(date -d "$$end" +%Y%m%d)" ]; do \
		$(MAKE) run DATE=$$d; \
		d=$$(date -d "$$d +1 day" +%Y-%m-%d); \
	done

test:
	python3 -m pytest tests/

dbt-snapshot:
	cd dbt_upi && dbt snapshot --profiles-dir .

dbt-run:
	cd dbt_upi && dbt run --profiles-dir .

dbt-test:
	cd dbt_upi && dbt test --profiles-dir .

dbt-seed:
	cd dbt_upi && dbt seed --profiles-dir .

clean:
	python -c "import pathlib, shutil; [shutil.rmtree(p) for p in pathlib.Path('.').rglob('__pycache__')]"
