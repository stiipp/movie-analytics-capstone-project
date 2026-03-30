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
)

select
	b.movie_id,
	to_char(b.release_date::date, 'YYYYMMDD')::int as date_key,
	b.budget,
	case
		when b.budget is null then 'Unknown'
		when b.budget < 10000000 then 'Low (<$10M)'
		when b.budget < 50000000 then 'Mid ($10M-$50M)'
		when b.budget < 100000000 then 'High ($50M-$100M)'
		else 'Blockbuster (>$100M)'
	end as budget_tier,
	b.revenue,
	case
		when b.is_roi_eligible then b.revenue - b.budget
		else null
	end as profit,
	case
		when b.is_roi_eligible and b.budget > 0 then (b.revenue - b.budget) / b.budget
		else null
	end as roi,
	b.is_budget_reported,
	b.is_revenue_reported,
	b.is_roi_eligible,
	b.avg_rating,
	b.total_ratings,
	b.std_dev
from base b

