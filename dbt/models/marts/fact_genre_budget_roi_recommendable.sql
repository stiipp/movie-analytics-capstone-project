{{ config(
    materialized='view',
    schema='gold',
    tags=['roi', 'genre', 'budget', 'powerbi', 'recommendation']
) }}

select
    *
from {{ ref('fact_genre_budget_roi') }}
where movie_count >= 5
  and genre not in ('TV Movie', 'Foreign')
