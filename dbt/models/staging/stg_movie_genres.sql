select
  movie_id,
  trim(genre) as genre
from {{ source('gold_prep', 'movie_genres') }}
where movie_id is not null
  and genre is not null
  and trim(genre) <> ''
