with countries as (
	select distinct
		country
	from {{ ref('bridge_movie_country') }}
	where country is not null
)

select
	dense_rank() over (order by country) as country_key,
	country
from countries

