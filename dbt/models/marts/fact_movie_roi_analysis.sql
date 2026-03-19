{{ config(
    materialized='view',
    schema='gold',
    tags=['roi', 'analysis']
) }}

-- View: ROI analysis for financially eligible investment candidates only
-- Excludes movies with missing/zero financials from the decision-making layer

with roi_movies as (
    select
        m.movie_id,
        m.title,
        m.release_date,
        f.budget::bigint as budget,
        f.budget_tier,
        f.budget_quartile,
        f.revenue::bigint as revenue,
        case
            when f.roi_pct >= 300 then 'Massive Hit (300%+)'
            when f.roi_pct >= 100 then 'Strong Hit (100-300%)'
            when f.roi_pct >= 0 then 'Profitable (0-100%)'
            else 'Loss (<0%)'
        end as profitability_tier,
        r.avg_rating,
        r.total_ratings,
        r.std_dev
    from {{ ref('stg_movies_roi_eligible') }} m
    join {{ ref('fact_movie_performance') }} f
        on m.movie_id = f.movie_id
    left join {{ ref('stg_ratings') }} r
        on m.movie_id = r.movie_id
    where f.is_roi_eligible = true
)

select
    roi_movies.movie_id,
    roi_movies.title,
    roi_movies.release_date,
    extract(year from roi_movies.release_date)::int as release_year,
    extract(month from roi_movies.release_date)::int as release_month,
    roi_movies.budget,
    roi_movies.budget_tier,
    roi_movies.budget_quartile,
    roi_movies.revenue,
    (roi_movies.revenue - roi_movies.budget) as profit,
    round(((roi_movies.revenue - roi_movies.budget)::numeric / roi_movies.budget::numeric * 100), 2) as roi_pct,
    roi_movies.profitability_tier,
    roi_movies.avg_rating,
    roi_movies.total_ratings,
    roi_movies.std_dev
from roi_movies
