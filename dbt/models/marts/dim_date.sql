with dates as (
	select distinct
		release_date::date as date_day
	from {{ ref('stg_movies') }}
	where release_date is not null
)

select
	to_char(date_day, 'YYYYMMDD')::int as date_key,
	date_day,
	extract(year from date_day)::int as year,
	extract(quarter from date_day)::int as quarter,
	extract(month from date_day)::int as month,
	to_char(date_day, 'Month') as month_name,
	extract(day from date_day)::int as day,
	extract(isodow from date_day)::int as day_of_week,
	case
		when extract(isodow from date_day) in (6, 7) then true
		else false
	end as is_weekend
from dates

