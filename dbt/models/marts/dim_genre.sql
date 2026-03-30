with genres as (
	select distinct
		genre
	from {{ ref('bridge_movie_genre') }}
	where genre is not null
)

select
	dense_rank() over (order by genre) as genre_key,
	genre
from genres

