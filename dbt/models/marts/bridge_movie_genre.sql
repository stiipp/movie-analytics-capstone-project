with src as (
    select
        movie_id,
        trim(genre) as genre
    from {{ ref('stg_movie_genres') }}
    where genre is not null
        and trim(genre) <> ''
)

select distinct
    s.movie_id,
    d.genre_key,
    d.genre
from src s
join {{ ref('dim_genre') }} d
    on s.genre = d.genre
