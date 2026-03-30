# Executive Summary

This capstone project builds an end-to-end movie analytics pipeline for a film investment use case. The business scenario is centered on a studio client that wants to move away from intuition-led greenlighting and toward a more evidence-based approach to capital allocation.

The pipeline ingests local raw project files and a local TMDB bulk enrichment file into PostgreSQL, enriches and standardizes the data with PySpark and pandas, and then models it with dbt into reporting-ready tables for Power BI. The architecture follows a medallion pattern:

- **Bronze:** raw source landing and enrichment outputs
- **Silver Base:** cleaned canonical movie, ratings, and extended-attribute tables
- **Gold Prep:** exploded movie-to-genre, movie-to-company, and movie-to-country mappings
- **Gold:** dimensional and analytical marts for reporting

The core analytical focus is investment-style movie performance. The project evaluates which genre and budget-tier combinations show the strongest historical ROI, which genres are gaining or losing momentum over time, and how much of the catalog is reliable enough for recommendation-grade analysis.

The warehouse is designed around a core movie-performance star schema:

- `fact_movie_performance`
- `dim_movie`
- `dim_date`
- `dim_budget_tier`

To keep the pipeline reusable beyond the current dashboard, the project also retains supporting genre, company, and country dimensions plus bridge tables.

The final deliverable is a Power BI dashboard built on the `gold` layer, supported by a documented and reproducible pipeline orchestrated in Apache Airflow.
