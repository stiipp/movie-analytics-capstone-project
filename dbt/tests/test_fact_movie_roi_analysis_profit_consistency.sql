-- Fails when profit is not equal to revenue - budget
select
  movie_id,
  budget,
  revenue,
  profit
from {{ ref('fact_movie_roi_analysis') }}
where profit is distinct from (revenue - budget)
