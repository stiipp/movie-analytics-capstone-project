PYTHON ?= python

# Host defaults for local runs (Docker-internal values are handled in container)
DB_HOST ?= localhost
DB_PORT ?= 5433
DB_USER ?= admin
DB_PASSWORD ?= admin
DB_NAME ?= capstone-movie-analytics
DB_SCHEMA ?= bronze

.PHONY: extract ingest pipeline

extract:
	$(PYTHON) scripts/processing/extract_data.py

ingest:
	DB_HOST=$(DB_HOST) DB_PORT=$(DB_PORT) DB_USER=$(DB_USER) DB_PASSWORD=$(DB_PASSWORD) DB_NAME=$(DB_NAME) DB_SCHEMA=$(DB_SCHEMA) \
	$(PYTHON) scripts/ingestion/ingest_movies.py

pipeline: extract ingest
