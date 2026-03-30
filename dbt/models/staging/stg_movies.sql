select
  id as movie_id,
  title,
  release_date::date as release_date,
  budget,
  budget_reported,
  is_budget_imputed,
  budget_imputation_method,
  revenue,
  is_budget_reported,
  is_revenue_reported,
  is_market_coverage_eligible,
  is_recommendation_eligible,
  -- Investment-analysis scope:
  -- use only reported budget and reported revenue for recommendation-grade ROI conclusions.
  (
    is_budget_reported
    and is_revenue_reported
    and budget >= 10000
    and revenue >= 1000000
  ) as is_roi_eligible
from {{ source('silver_base', 'movies') }}
where id is not null
