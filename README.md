# Movie Analytics Capstone Project
An end-to-end data pipeline that ingests raw movie data into PostgreSQL, enriches and cleans it with Spark and pandas, transforms it through dbt into analytical models, and orchestrates the workflow with Apache Airflow. The final `gold` layer is designed for Power BI reporting.

## Table of Contents

- [Overview](#overview)
- [Business Case & Questions](#business-case--questions)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Pipeline Flow](#data-pipeline-flow)
- [Data Sources](#data-sources)
- [dbt Models](#dbt-models)
- [Staging Layer](#staging-layer)
- [Marts Layer](#marts-layer)
- [Data Tests](#data-tests)
- [Airflow DAG](#airflow-dag)
- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Challenges & Solutions](#challenges--solutions)
- [Key Design Decisions](#key-design-decisions)

## Overview

This project builds a complete movie analytics pipeline for investment-style and market-coverage analysis. It demonstrates a modern data stack approach:

- **Ingest:** Python scripts extract raw project files and load source data into PostgreSQL `bronze` tables.
- **Enrich:** A Spark job joins in TMDB data, improves financial coverage, and creates enrichment audit fields.
- **Clean:** A pandas pipeline standardizes dates, financial fields, flags, and ratings into `silver_base`.
- **Transform:** A Spark transform step prepares movie-to-genre, movie-to-company, and movie-to-country mappings needed by downstream dbt models in `gold_prep`.
- **Model:** dbt builds staging, dimensional, fact, and trend models in `silver` and `gold`.
- **Test:** dbt validates core business logic and data quality rules.
- **Orchestrate:** Apache Airflow coordinates the full workflow through a single DAG.
- **Visualize:** The final `gold` layer is built for Power BI dashboarding.

## Business Case & Questions

This project is framed as a movie investment and portfolio-planning analytics case.

The main business problem is not just identifying high-revenue movies, but understanding which movie segments are attractive, repeatable, and reliable enough for decision-making.

Core business questions:

1. Which genre and budget-tier combinations deliver the strongest and most reliable ROI?
2. Which genres are gaining or losing momentum over time in revenue, ROI, and market coverage?
3. How much of the movie catalog is reliable enough for recommendation-grade investment analysis?

The project therefore uses two analytical scopes:

- **Recommendation Scope:** strict investment-style analysis using reported budget, reported revenue, budget >= `$10,000`, and revenue >= `$1,000,000`
- **Market Coverage Scope:** broader catalog analysis using positive reported revenue and budget >= `$10,000`, while allowing imputed budgets

## Architecture

The entire pipeline is designed to run in Docker. Key components:

- **Ingestion Layer:** `extract_data.py` and `ingest_movies.py` load raw project data and local TMDB bulk enrichment data into PostgreSQL `bronze`
- **Enrichment Layer:** `enrich_bronze_spark.py` joins TMDB enrichment, preserves lineage flags, and imputes budgets where appropriate
- **Cleaning Layer:** `clean_pandas.py` standardizes the movie, ratings, and extended-attribute datasets into `silver_base`
- **Transform Layer:** `transform_spark.py` generates `gold_prep` mapping tables for genres, production companies, and production countries
- **Modeling Layer:** dbt builds staging views in `silver` and reporting marts in `gold`
- **Orchestration Layer:** Airflow runs the full pipeline from extraction through dbt build
- **Visualization Layer:** Power BI connects to the final `gold` models

This project explicitly uses a medallion architecture pattern.

- **Bronze Layer:** raw landing layer for extracted project files and enrichment inputs in PostgreSQL `bronze`
- **Silver Layer:** cleaned and standardized analytical base layer in `silver_base`, plus dbt staging views in `silver`
- **Gold Layer:** business-facing reporting and dimensional models in `gold`, supported by Spark-prepared intermediate tables in `gold_prep`

In this project, the medallion design helps separate raw ingestion from cleaned business logic and final reporting outputs. That makes the pipeline easier to debug, rerun, test, and explain during analysis or dashboard handoff.

Schema layout:

- `bronze`: raw landing and enrichment outputs
- `silver_base`: cleaned canonical base tables
- `gold_prep`: Spark-prepared intermediate mapping tables for downstream dbt models
- `silver`: dbt staging views
- `gold`: dbt dimensions, facts, and Power BI-ready marts

## Tech Stack

| Tool | Purpose |
| --- | --- |
| Python 3.10 | Ingestion, extraction, cleaning, and transform scripts |
| pandas | CSV/JSON cleaning and standardization |
| PySpark 3.5.1 | Enrichment and transform jobs |
| PostgreSQL | Warehouse for bronze, silver, and gold schemas |
| dbt | Transformation, modeling, testing, and documentation |
| Apache Airflow 2.8.1 | Workflow orchestration |
| Docker & Docker Compose | Containerized infrastructure |
| Power BI | Final dashboard and reporting layer |

## Project Structure

```text
movie-analytics-capstone-project/
├── docker-compose.yml                 # Multi-service Docker setup
├── Makefile                           # Manual pipeline commands
├── .env.example                       # Example env vars
├── airflow/
│   ├── Dockerfile                     # Airflow image with dbt + Spark deps
│   ├── requirements.txt               # Python deps for Airflow containers
│   ├── dags/
│   │   └── movie_analytics_pipeline.py
│   ├── datasets/                      # Airflow-mounted dataset folder
│   └── logs/                          # Airflow task logs
├── data/
│   ├── raw/                           # Raw zip + TMDB CSV inputs
│   └── processed/                     # Extracted project files
├── dbt/
│   ├── dbt_project.yml                # dbt project config
│   ├── profiles.yml                   # dbt connection profiles
│   ├── packages.yml                   # dbt packages config
│   ├── analyses/                      # Advanced SQL deliverables
│   ├── macros/                        # Schema naming + cleanup macros
│   ├── models/
│   │   ├── staging/                   # Source docs + staging models
│   │   ├── marts/                     # Dimensions, facts, and analytical marts
│   └── tests/                         # Singular dbt tests
├── postgres/
│   └── init.sql                       # DB schema bootstrap
├── scripts/
│   ├── Dockerfile                     # Standalone Python image
│   ├── common/                        # Shared models/helpers
│   ├── ingestion/                     # Bronze ingestion
│   └── processing/                    # Extract, clean, enrich, transform
└── notebooks/
    └── movie_analytics_eda.ipynb      # Exploratory analysis notebook
```

## Data Pipeline Flow

```text
data/raw/project_data.zip
        │
        ▼
scripts/processing/extract_data.py
        │
        ▼
data/processed/{movies_main.csv, movie_extended.csv, ratings.json}
        │
        ▼
scripts/ingestion/ingest_movies.py
        │
        ▼
bronze.{movies_main, movie_extended, ratings}

data/raw/TMDB_movie_dataset_v11.csv
        │
        ▼
bronze.ext_tmdb_raw
        │
        ▼
scripts/processing/enrich_bronze_spark.py
        │
        ▼
bronze.movies_enriched + bronze.bronze_enrichment_audit
        │
        ▼
scripts/processing/clean_pandas.py
        │
        ▼
silver_base.{movies, ratings, movie_extended}
        │
        ▼
scripts/processing/transform_spark.py
        │
        ▼
gold_prep.{movie_genres, movie_companies, movie_countries}
        │
        ▼
dbt build
        │
        ▼
silver + gold reporting models
        │
        ▼
Power BI dashboard
```

Step 1 - Extract: `extract_data.py` extracts the raw project zip into `data/processed`, skipping files that already exist.

Step 2 - Ingest: `ingest_movies.py` loads processed CSV/JSON files plus the local TMDB bulk enrichment file into PostgreSQL `bronze` tables.

Step 3 - Enrich: `enrich_bronze_spark.py` joins TMDB enrichment to original movie rows, adds lineage flags, and supports budget imputation.

Step 4 - Clean: `clean_pandas.py` standardizes dates, numeric fields, and eligibility logic, then writes canonical tables to `silver_base`.

Step 5 - Transform: `transform_spark.py` produces exploded movie-to-genre, movie-to-company, and movie-to-country mappings in `gold_prep` for downstream dimensional modeling.

Step 6 - Model: dbt builds business-facing staging, dimensional, fact, and trend models in `silver` and `gold`.

Step 7 - Validate: dbt tests verify that the outputs are logically consistent.

## Data Sources

The pipeline processes internal project data plus a local TMDB bulk enrichment file.

| Source | Landing Table / Output | Description |
| --- | --- | --- |
| `data/raw/project_data.zip` | Extracted into `data/processed` | Main project source package |
| `movies_main.csv` | `bronze.movies_main` | Core movie-level financial and release data |
| `movie_extended.csv` | `bronze.movie_extended` | Extended movie attributes such as genres, companies, countries, languages |
| `ratings.json` | `bronze.ratings` | Nested movie ratings summary payload |
| `data/raw/TMDB_movie_dataset_v11.csv` | `bronze.ext_tmdb_raw` | Local bulk TMDB enrichment source |

## dbt Models

All source tables, models, and major columns are documented in YAML under `dbt/models/`. The dbt layer turns cleaned base tables into reporting-ready dimensional and analytical outputs.

### Staging Layer

Materialized as views in the `silver` schema.

| Model | Source | Purpose |
| --- | --- | --- |
| `stg_movies` | `silver_base.movies` | Standardizes movie-level financial and eligibility fields |
| `stg_ratings` | `silver_base.ratings` | Standardizes flattened movie ratings |
| `stg_movie_extended` | `silver_base.movie_extended` | Exposes extended movie attributes |
| `stg_movie_genres` | `gold_prep.movie_genres` | Cleans exploded movie-to-genre mappings |
| `stg_movie_companies` | `gold_prep.movie_companies` | Cleans exploded movie-to-company mappings |
| `stg_movie_countries` | `gold_prep.movie_countries` | Cleans exploded movie-to-country mappings |
| `stg_movies_roi_eligible` | `stg_movies` | Filters to recommendation-grade investment-analysis rows |

### Marts Layer

Built in the `gold` schema. This layer uses a core star schema for movie performance, keeps `dim_budget_tier` as a lightweight reporting lookup for Power BI, and also includes reusable genre, company, and country dimensions with bridge tables for broader downstream use.

#### Core Star Schema

| Model | Description |
| --- | --- |
| `dim_date` | Date dimension derived from ROI-eligible movie release dates |
| `dim_movie` | Movie dimension with release, financial, and rating attributes |
| `dim_budget_tier` | Lightweight budget-tier lookup dimension retained for Power BI slicing |
| `fact_movie_performance` | Core movie-grain fact table for recommendation-scope analysis |

The core star schema is centered on `fact_movie_performance`.

- **Grain:** one row per ROI-eligible movie
- **Dimension keys:** `movie_id` and `date_key`
- **Measures:** `budget`, `revenue`, `profit`, `roi`, `roi_pct`, and rating metrics

Conceptually, the design is:

```text
dim_date      dim_movie
    \           /
     \         /
   fact_movie_performance
```

This is the main dimensional model used for downstream reporting. `dim_budget_tier` is also kept as a small helper dimension for reporting convenience, since it is already used in Power BI.

#### Extended dimensional support

| Model | Description |
| --- | --- |
| `dim_genre` | Genre dimension used with `bridge_movie_genre` |
| `dim_company` | Production company dimension used with `bridge_movie_company` |
| `dim_country` | Production country dimension used with `bridge_movie_country` |
| `bridge_movie_genre` | Many-to-many bridge from movies to genres |
| `bridge_movie_company` | Many-to-many bridge from movies to production companies |
| `bridge_movie_country` | Many-to-many bridge from movies to production countries |

These models make the warehouse more reusable if the analytical focus shifts beyond the current dashboard, while keeping the main Power BI reporting flow centered on `fact_movie_performance` and the analytical marts.

#### Analytical marts

| Model | Description |
| --- | --- |
| `fact_movie_roi_analysis` | ROI-focused reporting view for financially eligible movies |
| `fact_movie_market_coverage` | Broader benchmark-scope reporting view for market-coverage analysis |
| `fact_genre_budget_roi` | Power BI-ready genre-budget ROI aggregate view |
| `fact_genre_budget_roi_recommendable` | Recommendation-grade subset of genre-budget ROI segments |
| `fact_genre_market_momentum` | Genre-year revenue and ROI trend view |
| `fact_genre_market_coverage_trends` | Genre-year market expansion and contraction trend view |

These marts are built from the core star schema plus the genre mapping layer, while the company and country dimensions remain available for future downstream analysis.

### Data Tests

The project includes both schema tests and singular tests.

Schema tests used across models:

- `unique`
- `not_null`
- `accepted_values`
- `relationships`

Business rules covered by singular tests:

| Test | Purpose |
| --- | --- |
| `test_stg_movies_roi_eligible_flag_consistency` | Ensures ROI-eligible staging rows satisfy expected financial thresholds |
| `test_fact_movie_performance_budget_quartile_consistency` | Validates budget quartile assignment logic |
| `test_fact_movie_roi_analysis_profit_consistency` | Ensures `profit = revenue - budget` |
| `test_fact_movie_roi_analysis_roi_pct_consistency` | Ensures ROI percentage is calculated correctly |
| `test_fact_genre_budget_roi_recommendable_min_sample` | Ensures recommendation-grade genre-budget slices have at least 5 movies |

## Airflow DAG

A single DAG, `movie_analytics_pipeline`, orchestrates the workflow using `BashOperator` for all tasks.

This DAG currently runs on a manual trigger (`schedule_interval=None`), which fits the capstone/demo workflow and avoids unnecessary automatic reruns while the project is still evolving.

| Setting | Value | Reason |
| --- | --- | --- |
| `schedule_interval` | `None` | Manual execution for controlled runs |
| `catchup` | `False` | Prevents backfill on first enable |
| `retries` | `1` | Retries each failed task once |
| `retry_delay` | `5 min` | Small retry buffer for transient failures |
| `max_active_runs` | `1` | Prevents overlapping full-pipeline runs |

Task sequence:

```text
extract_raw
   └──► ingest_raw
          └──► spark_enrich
                 └──► clean_pandas
                        └──► spark_transform
                               └──► dbt_build
```

dbt in Airflow writes logs and target artifacts to `/tmp` to avoid permission issues on mounted volumes.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Git
- Python 3.10+ if running scripts locally
### Setup

1. Clone the repository:

```bash
git clone https://github.com/<your-username>/movie-analytics-capstone-project.git
cd movie-analytics-capstone-project
```

2. Create your environment file:

```bash
cp .env.example .env
```

3. Place input files in the expected locations:

- `data/raw/project_data.zip`
- `data/raw/TMDB_movie_dataset_v11.csv` if using the bulk TMDB file

4. Start services:

```bash
docker compose up -d --build
```

This starts:

- PostgreSQL on port `5433`
- Airflow webserver on port `8080`
- Airflow scheduler
- `python-app` container
- `dbt` container

5. Access the Airflow UI:

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

## Running the Pipeline

### Option A: Via Airflow UI

Recommended for the full end-to-end workflow.

1. Open the Airflow UI
2. Enable `movie_analytics_pipeline`
3. Trigger the DAG manually
4. Monitor task progress in Graph or Grid view

### Option B: Manual pipeline commands

Run the project step by step from the host:

```bash
make extract
make ingest
make spark_enrich
make clean_pandas
make spark_transform
```

Build dbt models manually:

```bash
docker exec -it dbt-lab-capstone dbt build --profiles-dir . --target prod
```

### Option C: Manual dbt commands

```bash
docker exec -it dbt-lab-capstone bash
dbt build --profiles-dir . --target prod
dbt test --profiles-dir . --target prod
dbt docs generate --profiles-dir . --target prod
```

## Challenges & Solutions

### 1. Missing or weak financial coverage

Problem: Many movie records are not strong enough for ROI analysis if budget or revenue is missing.

Solution: Added a Spark enrichment step using TMDB data plus controlled budget imputation, while keeping lineage flags so reported and imputed values remain distinguishable.

### 2. Mixed date formats in the source data

Problem: Release dates appear in multiple formats, which can break downstream casting and modeling.

Solution: Implemented multi-format parsing in the pandas cleaning layer before writing canonical dates to `silver_base`.

### 3. Re-runs breaking downstream dependencies

Problem: Using destructive replace-style writes can drop and recreate tables, which breaks downstream references.

Solution: The pipeline uses truncate-and-reload patterns to preserve table objects while keeping reruns idempotent.

### 4. Need for both strict and broad analysis

Problem: A single eligibility rule was not enough. Strict ROI recommendations and broader market trend reporting need different scopes.

Solution: Split the logic into recommendation scope and market-coverage scope with separate flags and marts.

### 5. dbt default schema naming

Problem: dbt normally prefixes custom schemas with the target schema, which produces less readable names.

Solution: Added a custom `generate_schema_name` macro so schemas stay clean as `silver` and `gold` rather than prefixed variants.

### 6. Proving business logic consistency

Problem: It is not enough for the pipeline to run; the business rules behind ROI and eligibility also need validation.

Solution: Added schema tests and custom singular tests so business logic stays validated without adding extra audit-only pipeline layers.

## Key Design Decisions

### Why two analytical scopes?

The project separates strict recommendation analysis from broader market coverage because the business questions are different. Recommendation-grade conclusions require higher confidence, while trend and coverage reporting can tolerate a wider benchmark set.

### Why relative budget quartiles instead of fixed budget bands?

Budget quartiles adapt to the actual distribution of the dataset. This makes "low" and "high" budget segments meaningful within the project data instead of relying on arbitrary fixed thresholds.

### Why use both Spark and pandas?

Spark is used where distributed joins and large-table transformations are helpful, especially for enrichment and exploded mappings. pandas is used for targeted cleaning logic that is simpler and more readable in Python, especially mixed date parsing and JSON flattening.

### Why use BashOperator for all Airflow tasks?

Using `BashOperator` keeps the DAG consistent and simple. Every stage is executed as a subprocess, which makes orchestration straightforward and avoids packaging/import complexity inside Airflow.

### Why truncate-and-reload instead of table replace?

Replacing tables can drop objects and break downstream dependencies. Truncate-and-reload preserves structure while still making reruns idempotent.

### Why use a star-schema-style reporting layer?

The dimensional core makes the project easier to query, explain, and connect to BI tools. It also keeps the business-facing layer understandable for dashboards and capstone presentation.

### Why keep separate schemas by layer?

Distinct schemas make it clear which data is raw, cleaned, intermediate, and analyst-ready. This improves maintainability and matches standard warehouse and dbt practices.
