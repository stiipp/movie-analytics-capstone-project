with base as (
    select
        m.movie_id,
        m.release_date,
        m.budget,
        m.revenue,
        m.is_budget_reported,
        m.is_revenue_reported,
        m.is_roi_eligible,
        r.avg_rating,
        r.total_ratings,
        r.std_dev
    from {{ ref('stg_movies') }} m
    left join {{ ref('stg_ratings') }} r
        on m.movie_id = r.movie_id
    where m.is_recommendation_eligible = true
),
positive_budget_ranked as (
    select
        b.movie_id,
        ntile(4) over (order by b.budget) as budget_quartile
    from base b
    where b.budget is not null
      and b.budget > 0
),
budget_ranked as (
    select
        b.*,
        pbr.budget_quartile
    from base b
    left join positive_budget_ranked pbr
        on b.movie_id = pbr.movie_id
)

select
    b.movie_id,
    to_char(b.release_date::date, 'YYYYMMDD')::int as date_key,
    b.budget,
    case
        when b.budget_quartile = 1 then 'Low (Bottom Quartile)'
        when b.budget_quartile = 2 then 'Lower-Mid (Second Quartile)'
        when b.budget_quartile = 3 then 'Upper-Mid (Third Quartile)'
        when b.budget_quartile = 4 then 'High (Top Quartile)'
        else null
    end as budget_tier,
    b.budget_quartile,
    b.revenue,
    'Verified Financials' as financial_data_status,
    b.revenue - b.budget as profit,
    (b.revenue - b.budget)::numeric / b.budget as roi,
    round((((b.revenue - b.budget)::numeric / b.budget) * 100)::numeric, 2) as roi_pct,
    b.is_budget_reported,
    b.is_revenue_reported,
    b.is_roi_eligible,
    b.avg_rating,
    b.total_ratings,
    b.std_dev
from budget_ranked b
