with tiers as (
    select distinct
        budget_tier,
        budget_quartile
    from {{ ref('fact_movie_performance') }}
    where budget_tier is not null
)

select
    dense_rank() over (order by budget_quartile) as budget_tier_key,
    budget_tier,
    budget_quartile,
    case
        when budget_quartile = 1 then 'Bottom quarter of reported budgets in the analysis dataset'
        when budget_quartile = 2 then 'Second quarter of reported budgets in the analysis dataset'
        when budget_quartile = 3 then 'Third quarter of reported budgets in the analysis dataset'
        when budget_quartile = 4 then 'Top quarter of reported budgets in the analysis dataset'
        else 'Budget tier not classified'
    end as budget_tier_definition
from tiers
