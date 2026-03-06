PYTHON ?= python

# Host defaults for local runs (Docker-internal values are handled in container)
DB_HOST ?= localhost
DB_PORT ?= 5433
DB_USER ?= admin
DB_PASSWORD ?= admin
DB_NAME ?= capstone-movie-analytics
DB_SCHEMA ?= bronze

.PHONY: extract ingest pipeline

PANDAS_SCHEMA ?= silver_base
SPARK_SOURCE_SCHEMA ?= silver_base
SPARK_TARGET_SCHEMA ?= gold_prep

extract:
	$(PYTHON) scripts/processing/extract_data.py

ingest:
	DB_HOST=$(DB_HOST) DB_PORT=$(DB_PORT) DB_USER=$(DB_USER) DB_PASSWORD=$(DB_PASSWORD) DB_NAME=$(DB_NAME) DB_SCHEMA=$(DB_SCHEMA) \
	$(PYTHON) scripts/ingestion/ingest_movies.py

pipeline: extract ingest

.PHONY: clean_pandas spark_transform full_pipeline

clean_pandas:
	DB_HOST=$(DB_HOST) DB_PORT=$(DB_PORT) DB_USER=$(DB_USER) DB_PASSWORD=$(DB_PASSWORD) DB_NAME=$(DB_NAME) DB_SCHEMA=$(PANDAS_SCHEMA) \
	$(PYTHON) scripts/processing/clean_pandas.py

spark_transform:
	DB_HOST=$(DB_HOST) DB_PORT=$(DB_PORT) DB_USER=$(DB_USER) DB_PASSWORD=$(DB_PASSWORD) DB_NAME=$(DB_NAME) SOURCE_SCHEMA=$(SPARK_SOURCE_SCHEMA) TARGET_SCHEMA=$(SPARK_TARGET_SCHEMA) \
	$(PYTHON) scripts/processing/transform_spark.py

full_pipeline: extract ingest clean_pandas spark_transform
