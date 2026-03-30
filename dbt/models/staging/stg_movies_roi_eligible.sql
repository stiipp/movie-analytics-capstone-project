{{ config(
    materialized='view',
    schema='silver',
    tags=['roi', 'movies']
) }}

-- Staging view: investment-analysis scope only
-- Keeps only movies with sufficiently reliable financials for ROI and budget allocation analysis

select
    movie_id,
    title,
    release_date,
    budget,
    revenue,
    is_budget_reported,
    is_revenue_reported,
    is_recommendation_eligible,
    is_roi_eligible
from {{ ref('stg_movies') }}
where is_recommendation_eligible = true
  and movie_id is not null
