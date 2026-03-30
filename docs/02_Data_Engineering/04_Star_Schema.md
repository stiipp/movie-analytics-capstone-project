
## 4.1 Design Goal

The warehouse is designed to support a movie investment use case, not just generic movie reporting. For that reason, the core dimensional model is centered on movie-level financial performance, while still keeping supporting dimensions for genre, company, and country available for broader downstream analysis.

## 4.2 Core Star Schema

The main reporting star schema is:

- `gold.fact_movie_performance`
- `gold.dim_movie`
- `gold.dim_date`
- `gold.dim_budget_tier`

### `fact_movie_performance`

Grain:
- one row per ROI-eligible movie

Main measures:
- `budget`
- `revenue`
- `profit`
- `roi`
- `roi_pct`
- rating summary fields

Main keys:
- `movie_id`
- `date_key`

### `dim_movie`

Purpose:
- provide descriptive movie context for the core fact

Main attributes:
- title
- release date
- release year
- release month
- budget tier
- budget quartile
- rating fields

### `dim_date`

Purpose:
- provide release-date calendar context

Main attributes:
- full date
- year
- quarter
- month
- month name
- weekend flag

### `dim_budget_tier`

Purpose:
- provide a stable reporting lookup for budget segmentation

Why it is kept:
- it is already used in Power BI
- it makes slicing budget groups easier than relying only on raw labels in the fact table

## 4.3 Extended Dimensional Support

The project also retains reusable many-to-many structures for descriptive analysis beyond the core dashboard.

### Genre

- `gold.dim_genre`
- `gold.bridge_movie_genre`

Purpose:
- support genre slicing without duplicating movie rows in the main fact table

### Company

- `gold.dim_company`
- `gold.bridge_movie_company`

Purpose:
- support production-company analysis and future studio-focused reporting

### Country

- `gold.dim_country`
- `gold.bridge_movie_country`

Purpose:
- support production-country analysis and possible geographic expansion later

## 4.4 Relationship Logic

The design uses two patterns:

- **One-to-many:** core fact to core dimensions
- **Many-to-many via bridges:** movie to genre, company, and country

This keeps the main fact table compact while preserving analytical flexibility.

Conceptually, the model looks like this:

```text
dim_date      dim_movie      dim_budget_tier
    \           |              /
     \          |             /
      \         |            /
        fact_movie_performance
               |
     -------------------------
     |           |           |
bridge_movie_genre  bridge_movie_company  bridge_movie_country
     |           |           |
 dim_genre    dim_company   dim_country
```

## 4.5 Why This Is a Good Fit

This structure works well for the project because:

- the main business questions are movie-performance driven
- genres, companies, and countries are genuinely many-to-many
- the dashboard can stay centered on a clean movie-grain fact
- the warehouse stays reusable if the business focus changes later

## 4.6 Analytical Marts on Top of the Star Schema

Several business-facing marts are built on top of the core star schema and staging layer:

- `fact_movie_roi_analysis`
- `fact_movie_market_coverage`
- `fact_genre_budget_roi`
- `fact_genre_budget_roi_recommendable`
- `fact_genre_market_momentum`
- `fact_genre_market_coverage_trends`

These are not replacement facts for the core star schema. They are report-oriented analytical marts that summarize and reshape the underlying dimensional model for specific business questions.

## 4.7 Tradeoff and Final Decision

The project could have been reduced to only the dimensions used immediately in Power BI. Instead, it keeps the extra genre, company, and country dimensions because they still go through the cleaned pipeline and remain useful for future downstream analysis.

That gives the project a better balance:

- focused enough for the current capstone dashboard
- broad enough to remain reusable later
