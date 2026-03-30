select
  movie_id,
  trim(country) as country
from {{ source('gold_prep', 'movie_countries') }}
where movie_id is not null
  and country is not null
  and trim(country) <> ''
