{{ config(
    materialized='view',
    schema='gold',
    tags=['roi', 'genre', 'budget', 'powerbi']
) }}

with movie_genre_base as (
    select
        g.genre,
        f.budget_tier,
        f.budget_quartile,
        f.movie_id,
        f.budget,
        f.revenue,
        f.profit,
        f.roi,
        f.roi_pct,
        d.release_year
    from {{ ref('fact_movie_performance') }} f
    join {{ ref('stg_movie_genres') }} g
      on f.movie_id = g.movie_id
    join {{ ref('dim_movie') }} d
      on f.movie_id = d.movie_id
    where f.is_roi_eligible = true
      and f.roi is not null
      and f.budget_tier <> 'Unknown'
),
aggregated as (
    select
        genre,
        budget_tier,
        budget_quartile,
        count(*) as movie_count,
        sum(revenue) as total_revenue,
        sum(profit) as total_profit,
        round(avg(roi_pct), 2) as avg_roi_pct,
        percentile_cont(0.5) within group (order by roi_pct) as median_roi_pct,
        stddev_samp(roi_pct) as roi_stddev_pct,
        count(*) filter (where profit > 0) as profitable_movies,
        round(
            (
                count(*) filter (where profit > 0)::numeric
                / nullif(count(*), 0)
            ) * 100,
            2
        ) as success_rate_pct,
        min(release_year) as first_release_year,
        max(release_year) as last_release_year
    from movie_genre_base
    group by genre, budget_tier, budget_quartile
)

select
    md5(concat_ws('||', genre, budget_tier)) as genre_budget_profile_key,
    genre,
    budget_tier,
    budget_quartile,
    movie_count,
    total_revenue,
    total_profit,
    avg_roi_pct,
    round(median_roi_pct::numeric, 2) as median_roi_pct,
    round(roi_stddev_pct::numeric, 2) as roi_stddev_pct,
    profitable_movies,
    success_rate_pct,
    case
        when roi_stddev_pct is null then 'Insufficient History'
        when roi_stddev_pct <= 50 then 'Low Volatility'
        when roi_stddev_pct <= 125 then 'Moderate Volatility'
        else 'High Volatility'
    end as risk_band,
    case
        when roi_stddev_pct is null then 'Low Confidence'
        when roi_stddev_pct <= 50 and success_rate_pct >= 70 then 'Highly Predictable'
        when roi_stddev_pct <= 125 and success_rate_pct >= 55 then 'Moderately Predictable'
        else 'Volatile'
    end as predictability_label,
    first_release_year,
    last_release_year,
    dense_rank() over (
        order by median_roi_pct desc nulls last, success_rate_pct desc nulls last, total_revenue desc nulls last
    ) as median_roi_rank,
    dense_rank() over (
        partition by genre
        order by median_roi_pct desc nulls last, success_rate_pct desc nulls last, total_revenue desc nulls last
    ) as rank_within_genre,
    dense_rank() over (
        partition by budget_tier
        order by median_roi_pct desc nulls last, success_rate_pct desc nulls last, total_revenue desc nulls last
    ) as rank_within_budget_tier
from aggregated
