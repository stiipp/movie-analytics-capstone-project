select
  id as movie_id,
  genres,
  production_companies,
  production_countries,
  spoken_languages
from {{ source('silver_base', 'movie_extended') }}
where id is not null
