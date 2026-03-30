select
  id as movie_id,
  title,
  release_date::date as release_date,
  budget,
  revenue,
  is_budget_reported,
  is_revenue_reported,
  is_roi_eligible
from {{ source('silver_base', 'movies') }}
where id is not null
