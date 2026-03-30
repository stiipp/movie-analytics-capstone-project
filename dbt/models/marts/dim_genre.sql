with genres as (
    select distinct
        trim(genre) as genre
    from {{ ref('stg_movie_genres') }}
    where genre is not null
        and trim(genre) <> ''
)

select
    dense_rank() over (order by genre) as genre_key,
    genre
from genres
