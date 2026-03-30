
## 2.1 Dataset Overview

![Dataset overview profiling screenshot](<../99_Attachments/Pasted image 20260313131654.png>)

The source package extracted from `project_data.zip` is organized into three analytical inputs:

- `movies_main`: core movie-level financial and release fields
- `movie_extended`: descriptive and multi-value attributes such as genres, production companies, production countries, and spoken languages
- `ratings`: movie-level rating aggregates and last-rated timestamps

These three files are sufficient to support both the investment-focused marts and the broader reusable star schema.

## 2.2 Row and Column Summary

The original profiling work showed:

- `movies_main`: 45,486 rows
- `movie_extended`: 45,466 rows
- `ratings`: 9,066 rows

This immediately suggested two design realities:

- the movie catalog is large enough for segmentation analysis
- ratings coverage is partial, so rating-based insights must be treated as supplementary rather than universal

## 2.3 Schema and Type Audit

![Schema and type audit screenshot](<../99_Attachments/Pasted image 20260313132024.png>)

The raw files contained mixed data types and inconsistent formatting. The most important type issues were:

- movie identifiers represented inconsistently across files
- budget and revenue fields requiring numeric coercion
- release dates stored in mixed formats
- multi-value text fields that needed token normalization before they could support bridge tables

This profiling drove the cleaning rules later implemented in the pipeline:

- IDs standardized to a single joinable type
- financial fields coerced to numeric
- dates parsed through deterministic multi-format logic
- multivalue attributes trimmed and standardized before explode

## 2.4 Missingness and Completeness

![Missingness audit screenshot](<../99_Attachments/Pasted image 20260313132448.png>)

The profiling phase showed that completeness was uneven across domains:

- budget fields had meaningful gaps and zero inflation
- `production_companies` had the heaviest missingness among descriptive fields
- ratings fields were high quality where present, but not available for the full catalog

The practical implication was that the project needed two analytical scopes:

- a **strict recommendation scope** for stronger investment conclusions
- a **broader market coverage scope** for trend and context analysis

## 2.5 Key Integrity and Duplicates

![Duplicate key profiling screenshot](<../99_Attachments/Pasted image 20260313133428.png>)
![Duplicate record detail screenshot](<../99_Attachments/Pasted image 20260313133443.png>)

Profiling also surfaced duplicate movie identifiers and a small number of exact duplicates in the source tables. These findings mattered because the final reporting layer depends on clean movie-grain facts and stable many-to-many bridge relationships.

The response in the pipeline was:

- deterministic canonical row selection during cleaning
- explicit normalization before dimensional modeling
- separation of raw source state from cleaned analytical state through the medallion layers

## 2.6 Cross-File Join Coverage

![Cross-file referential integrity screenshot](<../99_Attachments/Pasted image 20260313133806.png>)

The most important cross-file issue was ratings coverage. Many `ratings.movie_id` values did not match the core movie dataset. That means ratings are analytically useful, but they cannot be treated as a complete enrichment for the full catalog.

This influenced the final design in two ways:

- rating fields are retained in the warehouse because they add value where available
- movie-performance and recommendation logic do not depend on ratings completeness

## 2.7 Date Quality

![Date quality audit screenshot](<../99_Attachments/Pasted image 20260313134810.png>)

Release dates appeared in multiple formats, but the deterministic parsing strategy succeeded without introducing unresolved date failures in the profiled non-null set.

The resulting rule adopted in the cleaning layer was:

- parse with ordered multi-format passes
- preserve failures as null instead of guessing silently
- store cleaned dates in a canonical format for downstream dimensional modeling

## 2.8 Financial Quality

![Financial anomaly profiling screenshot](<../99_Attachments/Pasted image 20260313134743.png>)

Financial profiling revealed two major issues:

- high zero inflation in both budget and revenue
- a large number of rows where zero likely meant unknown or unreported, not truly zero economics

That finding directly shaped the business logic:

- recommendation-grade ROI analysis uses only reported financials that meet threshold rules
- market-coverage analysis allows a broader but clearly labeled subset
- ROI is never treated as meaningful without adequate financial quality

## 2.9 Multi-Value Attribute Profiling

![Multi-value token quality screenshot](<../99_Attachments/Pasted image 20260313140549.png>)
![Category distribution screenshot](<../99_Attachments/Pasted image 20260313140240.png>)
![Cardinality per movie screenshot](<../99_Attachments/Pasted image 20260313135952.png>)

The extended metadata confirmed that the dataset contains genuine many-to-many relationships:

- movies can belong to multiple genres
- movies can have multiple production companies
- movies can have multiple production countries

This justified the use of:

- exploded prep mappings in `gold_prep`
- bridge tables in the dbt marts layer
- reusable dimensions for genre, company, and country

## 2.10 Ratings Quality

The ratings dataset itself was structurally strong:

- average rating values were valid
- total rating counts were non-negative
- timestamps were parseable

The limitation was not quality within the file. The limitation was coverage against the main movie catalog.

## 2.11 Profiling Implications for the Model

![Financial and statistical profiling screenshot](<../99_Attachments/Pasted image 20260313140705.png>)

The profiling phase led directly to the final warehouse design:

- **Core star schema:** centered on movie-grain performance
- **Budget tiering:** based on relative quartiles of valid reported budgets
- **Bridge tables:** required for genre, company, and country relationships
- **Two analytical scopes:** recommendation-grade and market-coverage
- **Transparency:** financial reliability is treated as part of the business logic, not hidden in preprocessing

## 2.12 Summary

The profiling work showed that the dataset is useful, but only if it is modeled carefully. The strongest risks were duplicate movie IDs, zero-heavy financial fields, and incomplete ratings coverage. The pipeline design, medallion structure, and final dbt models were all chosen to handle those issues explicitly rather than hide them.
