-- Week 1 SQL Deliverables
-- Purpose: Evidence for advanced SQL requirements (LAG/LEAD/ROW_NUMBER)
-- Source tables: gold.fact_movie_performance, silver.stg_movie_genres

-- ============================================================
-- 1) Monthly revenue changes using LAG
-- ============================================================
with monthly as (
    select
        date_trunc('month', d.date_day)::date as month_start,
        sum(f.revenue) as monthly_revenue
    from gold.fact_movie_performance f
    join gold.dim_date d
      on f.date_key = d.date_key
    where f.revenue is not null
    group by 1
)
select
    month_start,
    monthly_revenue,
    lag(monthly_revenue) over (order by month_start) as prev_month_revenue,
    monthly_revenue - lag(monthly_revenue) over (order by month_start) as month_over_month_change,
    case
        when lag(monthly_revenue) over (order by month_start) > 0 then
            round(((monthly_revenue - lag(monthly_revenue) over (order by month_start))
                / lag(monthly_revenue) over (order by month_start))::numeric * 100, 2)
        else null
    end as month_over_month_pct
from monthly
order by month_start;

-- ============================================================
-- 2) Sequential movie performance using LAG/LEAD (per budget tier)
-- ============================================================
select
    f.movie_id,
    d.year as release_year,
    f.budget_tier,
    f.revenue,
    lag(f.revenue) over (
        partition by f.budget_tier
        order by d.year, f.movie_id
    ) as prev_movie_revenue_in_tier,
    lead(f.revenue) over (
        partition by f.budget_tier
        order by d.year, f.movie_id
    ) as next_movie_revenue_in_tier
from gold.fact_movie_performance f
join gold.dim_date d
  on f.date_key = d.date_key
where f.revenue is not null
order by f.budget_tier, d.year, f.movie_id;

-- ============================================================
-- 3) Year-over-year comparisons using LAG
-- ============================================================
with yearly as (
    select
        d.year as release_year,
        sum(f.revenue) as yearly_revenue,
        avg(f.roi) as avg_roi
    from gold.fact_movie_performance f
    join gold.dim_date d
      on f.date_key = d.date_key
    where f.revenue is not null
    group by 1
)
select
    release_year,
    yearly_revenue,
    lag(yearly_revenue) over (order by release_year) as prev_year_revenue,
    yearly_revenue - lag(yearly_revenue) over (order by release_year) as yoy_revenue_change,
    avg_roi,
    lag(avg_roi) over (order by release_year) as prev_year_avg_roi,
    avg_roi - lag(avg_roi) over (order by release_year) as yoy_avg_roi_change
from yearly
order by release_year;

-- ============================================================
-- 4) Movie rankings by genre using ROW_NUMBER
-- ============================================================
with genre_rank_base as (
    select
        g.genre,
        m.movie_id,
        dm.title,
        m.revenue,
        m.profit,
        row_number() over (
            partition by g.genre
            order by m.revenue desc nulls last, m.movie_id
        ) as revenue_rank_in_genre
    from gold.fact_movie_performance m
    join silver.stg_movie_genres g
      on m.movie_id = g.movie_id
    join gold.dim_movie dm
      on m.movie_id = dm.movie_id
    where m.revenue is not null
)
select *
from genre_rank_base
where revenue_rank_in_genre <= 10
order by genre, revenue_rank_in_genre;

-- ============================================================
-- 5) Top performers by year using ROW_NUMBER
-- ============================================================
with yearly_rank_base as (
    select
        d.year as release_year,
        f.movie_id,
        dm.title,
        f.revenue,
        f.profit,
        row_number() over (
            partition by d.year
            order by f.revenue desc nulls last, f.movie_id
        ) as revenue_rank_in_year
    from gold.fact_movie_performance f
    join gold.dim_date d
      on f.date_key = d.date_key
    join gold.dim_movie dm
      on f.movie_id = dm.movie_id
    where f.revenue is not null
)
select *
from yearly_rank_base
where revenue_rank_in_year <= 10
order by release_year, revenue_rank_in_year;

-- ============================================================
-- 6) Query optimization checklist for evaluation evidence
-- Run these in PostgreSQL once; they improve filter/join/window performance.
-- ============================================================
-- create index if not exists idx_fact_movie_performance_date_key on gold.fact_movie_performance (date_key);
-- create index if not exists idx_fact_movie_performance_movie_id on gold.fact_movie_performance (movie_id);
-- create index if not exists idx_fact_movie_performance_revenue on gold.fact_movie_performance (revenue);
-- create index if not exists idx_stg_movie_genres_movie_id on silver.stg_movie_genres (movie_id);
-- create index if not exists idx_dim_date_year on gold.dim_date (year);

-- Explain plan template (replace <query>):
-- explain analyze <query>;
