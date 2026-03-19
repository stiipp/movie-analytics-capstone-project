select
    genre_budget_profile_key,
    genre,
    budget_tier,
    movie_count
from {{ ref('fact_genre_budget_roi_recommendable') }}
where movie_count < 5
