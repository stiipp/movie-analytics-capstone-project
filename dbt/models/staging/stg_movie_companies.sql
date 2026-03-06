select
  movie_id,
  trim(company) as company
from {{ source('gold_prep', 'movie_companies') }}
where movie_id is not null
  and company is not null
  and trim(company) <> ''
