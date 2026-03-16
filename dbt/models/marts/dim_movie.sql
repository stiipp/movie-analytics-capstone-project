select
	m.movie_id,
	m.title,
	m.release_date,
	extract(year from m.release_date)::int as release_year,
	extract(month from m.release_date)::int as release_month,
	m.budget,
	case
		when m.budget is null then 'Unknown'
		when m.budget < 10000000 then 'Low (<$10M)'
		when m.budget < 50000000 then 'Mid ($10M-$50M)'
		when m.budget < 100000000 then 'High ($50M-$100M)'
		else 'Blockbuster (>$100M)'
	end as budget_tier,
	m.revenue,
	case
		when m.is_roi_eligible then m.revenue - m.budget
		else null
	end as profit,
	case
		when m.is_roi_eligible and m.budget > 0 then (m.revenue - m.budget) / m.budget
		else null
	end as roi,
	m.is_budget_reported,
	m.is_revenue_reported,
	m.is_roi_eligible,
	r.avg_rating,
	r.total_ratings,
	r.std_dev,
	r.last_rated
from {{ ref('stg_movies') }} m
left join {{ ref('stg_ratings') }} r
	on m.movie_id = r.movie_id

