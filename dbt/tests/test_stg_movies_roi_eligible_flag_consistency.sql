-- Fails when ROI-eligible staging rows violate the expected financial flags
select
  movie_id,
  budget,
  revenue,
  is_budget_reported,
  is_revenue_reported,
  is_roi_eligible
from {{ ref('stg_movies_roi_eligible') }}
where not (
    is_budget_reported
    and is_revenue_reported
    and is_roi_eligible
    and budget >= 10000
    and revenue >= 1000000
  )
