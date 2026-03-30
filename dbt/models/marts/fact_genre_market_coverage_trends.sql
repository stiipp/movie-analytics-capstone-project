{{ config(
    materialized='view',
    schema='gold',
    tags=['benchmark', 'trend', 'genre', 'powerbi']
) }}

with yearly_genre as (
    select
        g.genre,
        f.release_year,
        count(*) as movie_count,
        sum(f.revenue) as total_revenue,
        round(avg(f.budget)::numeric, 2) as avg_budget,
        percentile_cont(0.5) within group (order by f.revenue) as median_revenue,
        count(*) filter (
            where f.is_recommendation_eligible
        ) as recommendation_eligible_movie_count
    from {{ ref('fact_movie_market_coverage') }} f
    join {{ ref('stg_movie_genres') }} g
      on f.movie_id = g.movie_id
    group by g.genre, f.release_year
),
windowed as (
    select
        genre,
        release_year,
        movie_count,
        total_revenue,
        avg_budget,
        round(median_revenue::numeric, 2) as median_revenue,
        recommendation_eligible_movie_count,
        lag(movie_count) over (
            partition by genre
            order by release_year
        ) as prev_year_movie_count,
        lag(total_revenue) over (
            partition by genre
            order by release_year
        ) as prev_year_revenue,
        lead(movie_count) over (
            partition by genre
            order by release_year
        ) as next_year_movie_count
    from yearly_genre
)

select
    md5(concat_ws('||', genre, release_year::text)) as genre_market_coverage_year_key,
    genre,
    release_year,
    movie_count,
    total_revenue,
    avg_budget,
    median_revenue,
    recommendation_eligible_movie_count,
    prev_year_movie_count,
    movie_count - prev_year_movie_count as yoy_movie_count_change,
    case
        when prev_year_movie_count > 0 then round((((movie_count - prev_year_movie_count) / prev_year_movie_count::numeric) * 100)::numeric, 2)
        else null
    end as yoy_movie_count_change_pct,
    prev_year_revenue,
    total_revenue - prev_year_revenue as yoy_revenue_change,
    case
        when prev_year_revenue > 0 then round((((total_revenue - prev_year_revenue) / prev_year_revenue::numeric) * 100)::numeric, 2)
        else null
    end as yoy_revenue_change_pct,
    next_year_movie_count,
    case
        when prev_year_movie_count is null then 'Insufficient History'
        when movie_count > prev_year_movie_count and total_revenue >= coalesce(prev_year_revenue, total_revenue) then 'Expanding'
        when movie_count < prev_year_movie_count and total_revenue < coalesce(prev_year_revenue, total_revenue) then 'Contracting'
        else 'Mixed'
    end as market_trend_label
from windowed
