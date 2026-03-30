{{ config(
    materialized='view',
    schema='gold',
    tags=['trend', 'genre', 'powerbi']
) }}

with yearly_genre as (
    select
        g.genre,
        d.release_year,
        count(*) as movie_count,
        sum(f.revenue) as total_revenue,
        round(avg(f.roi_pct), 2) as avg_roi_pct,
        percentile_cont(0.5) within group (order by f.roi_pct) as median_roi_pct
    from {{ ref('fact_movie_performance') }} f
    join {{ ref('stg_movie_genres') }} g
      on f.movie_id = g.movie_id
    join {{ ref('dim_movie') }} d
      on f.movie_id = d.movie_id
    where f.is_roi_eligible = true
      and f.revenue is not null
      and f.roi_pct is not null
    group by g.genre, d.release_year
),
windowed as (
    select
        genre,
        release_year,
        movie_count,
        total_revenue,
        avg_roi_pct,
        round(median_roi_pct::numeric, 2) as median_roi_pct,
        lag(total_revenue) over (
            partition by genre
            order by release_year
        ) as prev_year_revenue,
        lag(median_roi_pct) over (
            partition by genre
            order by release_year
        ) as prev_year_median_roi_pct,
        lead(median_roi_pct) over (
            partition by genre
            order by release_year
        ) as next_year_median_roi_pct
    from yearly_genre
)

select
    md5(concat_ws('||', genre, release_year::text)) as genre_year_key,
    genre,
    release_year,
    movie_count,
    total_revenue,
    avg_roi_pct,
    median_roi_pct,
    prev_year_revenue,
    total_revenue - prev_year_revenue as yoy_revenue_change,
    case
        when prev_year_revenue > 0 then round((((total_revenue - prev_year_revenue) / prev_year_revenue::numeric) * 100)::numeric, 2)
        else null
    end as yoy_revenue_change_pct,
    round(prev_year_median_roi_pct::numeric, 2) as prev_year_median_roi_pct,
    round((median_roi_pct - prev_year_median_roi_pct)::numeric, 2) as yoy_median_roi_pct_change,
    round(next_year_median_roi_pct::numeric, 2) as next_year_median_roi_pct,
    case
        when prev_year_median_roi_pct is null then 'Insufficient History'
        when median_roi_pct > prev_year_median_roi_pct then 'Trending Up'
        when median_roi_pct < prev_year_median_roi_pct then 'Trending Down'
        else 'Stable'
    end as momentum_label
from windowed
