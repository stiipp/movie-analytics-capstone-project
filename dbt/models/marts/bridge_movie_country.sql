select
  movie_id,
  country
from {{ ref('stg_movie_countries') }}
