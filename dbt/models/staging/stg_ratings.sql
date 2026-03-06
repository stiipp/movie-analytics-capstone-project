select
  movie_id,
  avg_rating,
  total_ratings,
  std_dev,
  last_rated
from {{ source('silver_base', 'ratings') }}
where movie_id is not null
