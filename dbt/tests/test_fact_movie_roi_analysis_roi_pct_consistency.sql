-- Fails when roi_pct does not match rounded formula for positive budget rows
select
  movie_id,
  budget,
  revenue,
  roi_pct,
  round(((revenue - budget)::numeric / budget::numeric * 100), 2) as expected_roi_pct
from {{ ref('fact_movie_roi_analysis') }}
where budget > 0
  and roi_pct is distinct from round(((revenue - budget)::numeric / budget::numeric * 100), 2)
