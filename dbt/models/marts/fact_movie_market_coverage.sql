{{ config(
    materialized='view',
    schema='gold',
    tags=['benchmark', 'market', 'coverage']
) }}

select
    m.movie_id,
    m.title,
    m.release_date,
    extract(year from m.release_date)::int as release_year,
    extract(month from m.release_date)::int as release_month,
    (floor(extract(year from m.release_date)::numeric / 10) * 10)::int as release_decade,
    m.budget,
    m.revenue,
    m.is_budget_reported,
    m.is_revenue_reported,
    m.is_budget_imputed,
    m.budget_imputation_method,
    m.is_recommendation_eligible,
    'Market Coverage (10K budget / positive revenue)'::text as benchmark_scope_label,
    r.avg_rating,
    r.total_ratings,
    r.std_dev,
    r.last_rated,
    e.genres,
    e.production_companies,
    e.production_countries,
    e.spoken_languages
from {{ ref('stg_movies') }} m
left join {{ ref('stg_ratings') }} r
    on m.movie_id = r.movie_id
left join {{ ref('stg_movie_extended') }} e
    on m.movie_id = e.movie_id
where m.movie_id is not null
  and m.is_market_coverage_eligible = true
