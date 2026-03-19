select
    f.movie_id,
    m.title,
    m.release_date,
    extract(year from m.release_date)::int as release_year,
    extract(month from m.release_date)::int as release_month,
    f.budget,
    f.budget_tier,
    f.budget_quartile,
    f.revenue,
    f.profit,
    f.roi,
    f.roi_pct,
    m.is_budget_reported,
    m.is_revenue_reported,
    m.is_roi_eligible,
    r.avg_rating,
    r.total_ratings,
    r.std_dev,
    r.last_rated
from {{ ref('stg_movies') }} m
join {{ ref('fact_movie_performance') }} f
    on m.movie_id = f.movie_id
left join {{ ref('stg_ratings') }} r
    on m.movie_id = r.movie_id
