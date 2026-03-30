
## 5.1 Pipeline Logic

The project is organized as a layered pipeline where each step has one clear responsibility.

### Step 1: Extract

Script:
- `scripts/processing/extract_data.py`

Purpose:
- unpack the local project source archive into `data/processed`

Output:
- `movies_main.csv`
- `movie_extended.csv`
- `ratings.json`

### Step 2: Ingest

Script:
- `scripts/ingestion/ingest_movies.py`

Purpose:
- load the processed source files and the local TMDB bulk file into PostgreSQL bronze tables

Output:
- `bronze.movies_main`
- `bronze.movie_extended`
- `bronze.ratings`
- `bronze.ext_tmdb_raw`

### Step 3: Enrich

Script:
- `scripts/processing/enrich_bronze_spark.py`

Purpose:
- join TMDB enrichment data to the bronze movie data
- improve financial coverage
- add lineage and imputation-related fields

Important outcome:
- budget enrichment is handled more flexibly than recommendation logic, which is applied later

### Step 4: Clean

Script:
- `scripts/processing/clean_pandas.py`

Purpose:
- standardize movie, ratings, and extended-attribute data into canonical `silver_base` tables

Main cleaning rules:
- deduplicate movie IDs deterministically
- parse mixed-format dates
- coerce financials to numeric
- normalize missing and zero-heavy values
- flatten ratings payloads
- derive analytical eligibility flags

Output:
- `silver_base.movies`
- `silver_base.ratings`
- `silver_base.movie_extended`

### Step 5: Transform

Script:
- `scripts/processing/transform_spark.py`

Purpose:
- explode cleaned multivalue attributes into bridge-ready mapping tables

Output:
- `gold_prep.movie_genres`
- `gold_prep.movie_companies`
- `gold_prep.movie_countries`

This step exists because genres, companies, and countries are naturally many-to-many and are easier to model once they are normalized into one-row-per-mapping form.

### Step 6: Model

Tool:
- dbt

Purpose:
- build staging views, dimensions, bridge tables, and analytical marts in `silver` and `gold`

### Step 7: Orchestrate

Tool:
- Apache Airflow

Purpose:
- run the full pipeline in the correct sequence through a single DAG

## 5.2 Main Business Logic

### Recommendation Scope

This is the strict investment-analysis layer used for the main recommendation.

Rules:
- reported budget required
- reported revenue required
- budget >= `$10,000`
- revenue >= `$1,000,000`

This scope supports:
- `fact_movie_performance`
- `fact_movie_roi_analysis`
- `fact_genre_budget_roi`
- `fact_genre_budget_roi_recommendable`
- `fact_genre_market_momentum`

### Market Coverage Scope

This is the broader context layer used for catalog-level and market-trend analysis.

Rules:
- positive reported revenue
- budget >= `$10,000`
- imputed budget allowed

This scope supports:
- `fact_movie_market_coverage`
- `fact_genre_market_coverage_trends`

## 5.3 Budget Tier Logic

Budget segmentation is relative, not fixed-dollar.

The logic is:
- rank ROI-eligible movies by positive reported budget
- assign quartiles with `NTILE(4)`
- label the quartiles as budget tiers

Resulting labels:
- `Low (Bottom Quartile)`
- `Lower-Mid (Second Quartile)`
- `Upper-Mid (Third Quartile)`
- `High (Top Quartile)`

Why quartiles were chosen:
- they create more useful segmentation than a single low/mid/high split
- they preserve relative comparison inside the dataset
- they fit the current business question better than fixed dollar bands

## 5.4 Why Bridges Are Used

Genres, production companies, and production countries are not one-to-one with movies. A single movie can belong to several of each.

That means the project needs:
- staging mappings in `gold_prep`
- cleaned staging views in `silver`
- dimensions plus bridge tables in `gold`

This prevents the main movie fact from being duplicated across descriptive categories.

## 5.5 Why Some Extra Models Are Kept

The current Power BI dashboard is mainly centered on:

- `fact_movie_performance`
- `fact_genre_budget_roi`
- `fact_genre_market_momentum`

Even so, the project also keeps:

- `dim_company`
- `dim_country`
- their bridge tables

Those models are still useful because they are already cleaned and modeled properly. If a later version of the project shifts toward studio analysis, country analysis, or another downstream question, the warehouse can support that without returning to raw data design.

## 5.6 Code Quality Choices

Several cleanup choices were made to keep the codebase simpler and safer:

- local-only ingestion path retained
- optional API extraction removed
- unused audit-only pieces removed
- safer write logic used for pipeline table refreshes
- stale model references cleaned up
- documentation aligned with the final schema

## 5.7 Final Engineering Principle

The project does not try to clean every possible field for every possible use case. Instead, it cleans and models the parts of the data that are meaningful for downstream analysis, while still preserving a few extra dimensions that are already useful and low-cost to keep.

That is the core design principle behind the final codebase: focused, reusable, and straightforward.
