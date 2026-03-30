
## 6.1 Dashboard Objective

The dashboard is designed to answer the capstone business case in a decision-oriented way:

> Which genre and budget-tier combinations appear most attractive for movie investment, and how reliable are those signals?

The report should not behave like a generic movie catalog explorer. It should guide the viewer from high-level investment framing into more specific segment recommendations and supporting evidence.

## 6.2 Recommended Dataset Inputs

Core reporting inputs:

- `gold.fact_movie_performance`
- `gold.fact_movie_roi_analysis`
- `gold.fact_movie_market_coverage`
- `gold.fact_genre_budget_roi`
- `gold.fact_genre_budget_roi_recommendable`
- `gold.fact_genre_market_momentum`
- `gold.fact_genre_market_coverage_trends`

Supporting dimensions and bridges:

- `gold.dim_date`
- `gold.dim_movie`
- `gold.dim_budget_tier`
- `gold.dim_genre`
- `gold.bridge_movie_genre`
- `gold.dim_company`
- `gold.bridge_movie_company`
- `gold.dim_country`
- `gold.bridge_movie_country`

## 6.3 Recommended Power BI Model

Keep `gold.fact_movie_performance` as the central fact table.

Recommended core relationships:

1. `fact_movie_performance[date_key] -> dim_date[date_key]`
2. `fact_movie_performance[movie_id] -> dim_movie[movie_id]`
3. `fact_movie_performance[budget_tier] -> dim_budget_tier[budget_tier]`

Recommended bridge relationships:

4. `bridge_movie_genre[movie_id] -> fact_movie_performance[movie_id]`
5. `bridge_movie_genre[genre_key] -> dim_genre[genre_key]`
6. `bridge_movie_company[movie_id] -> fact_movie_performance[movie_id]`
7. `bridge_movie_company[company_key] -> dim_company[company_key]`
8. `bridge_movie_country[movie_id] -> fact_movie_performance[movie_id]`
9. `bridge_movie_country[country_key] -> dim_country[country_key]`

Suggested modeling note:
- keep relationship direction simple and controlled
- avoid rebuilding warehouse logic in DAX when the same logic already exists in dbt

## 6.4 Core Measures

These are the core measures the dashboard should expose.

```DAX
Total Movies =
DISTINCTCOUNT('fact_movie_performance'[movie_id])

Total Revenue =
SUM('fact_movie_performance'[revenue])

Total Budget =
SUM('fact_movie_performance'[budget])

Total Profit =
SUM('fact_movie_performance'[profit])

Median ROI % =
MEDIAN('fact_movie_performance'[roi_pct])

Average ROI % =
AVERAGE('fact_movie_performance'[roi_pct])

Recommendation Movies =
CALCULATE(
    DISTINCTCOUNT('fact_movie_performance'[movie_id]),
    'fact_movie_performance'[is_roi_eligible] = TRUE()
)

YoY Revenue Change =
[Total Revenue]
    - CALCULATE(
        [Total Revenue],
        DATEADD('dim_date'[date_day], -1, YEAR)
    )

YoY Revenue Change % =
DIVIDE(
    [YoY Revenue Change],
    CALCULATE([Total Revenue], DATEADD('dim_date'[date_day], -1, YEAR)),
    0
)
```

## 6.5 Recommended Report Pages

### Page 1: Executive Overview

Purpose:
- show the main recommendation-scope picture quickly

Suggested visuals:
- KPI cards for total movies, total revenue, total profit, median ROI, average ROI
- bar chart for budget-tier performance
- short text callout summarizing the strongest budget-level takeaway

### Page 2: Genre Opportunity Analysis

Purpose:
- identify the strongest recommendation-grade genre-budget combinations

Suggested visuals:
- heatmap of genre by budget tier
- ranked bar chart of top genre-budget combinations
- bubble or scatter view showing ROI, total revenue, and sample size together

Best source:
- `fact_genre_budget_roi_recommendable`

### Page 3: Market Momentum and Coverage

Purpose:
- show whether promising genres are still relevant in the broader market

Suggested visuals:
- yearly trend chart from `fact_genre_market_momentum`
- market expansion/contraction chart from `fact_genre_market_coverage_trends`
- comparison view of recommendation scope vs broader market coverage

### Page 4: Movie Detail or Supporting Drilldown

Purpose:
- give the user a lower-level inspection layer without overcrowding the main pages

Suggested visuals:
- movie table using `fact_movie_roi_analysis`
- filters by genre, budget tier, year, and profitability band
- optional company or country cuts if needed

## 6.6 Interactivity

Recommended slicers:

- year
- budget tier
- genre
- company
- country

Recommended interaction features:

- drill-through from summary pages to movie-level detail
- tooltips with budget, revenue, profit, ROI, and rating context
- bookmarks only if they make the story clearer, not just more complex

## 6.7 Reporting Principle

The dashboard should stay aligned with the warehouse design:

- use the core star schema for stable filtering and navigation
- use analytical marts for presentation-ready business views
- keep DAX focused on measures, not on rebuilding SQL transformations

## 6.8 Final Design Note

The strongest version of this dashboard is not the one with the most visuals. It is the one that most clearly answers the three business questions and shows why the recommendation is credible.
