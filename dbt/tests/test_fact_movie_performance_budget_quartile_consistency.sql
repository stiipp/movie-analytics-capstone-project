-- Fails when positive-budget rows are missing a quartile or non-positive budgets are assigned one.
select
  movie_id,
  budget,
  budget_quartile,
  budget_tier
from {{ ref('fact_movie_performance') }}
where (
    budget is not null
    and budget > 0
    and budget_quartile is null
  )
  or (
    (budget is null or budget <= 0)
    and budget_quartile is not null
  )
