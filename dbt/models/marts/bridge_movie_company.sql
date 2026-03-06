select
  movie_id,
  company
from {{ ref('stg_movie_companies') }}
