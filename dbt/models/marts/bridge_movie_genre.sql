select
  movie_id,
  genre
from {{ ref('stg_movie_genres') }}
