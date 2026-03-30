with src as (
    select
        movie_id,
        trim(country) as country
    from {{ ref('stg_movie_countries') }}
    where country is not null
        and trim(country) <> ''
)

select distinct
    s.movie_id,
    d.country_key,
    d.country
from src s
join {{ ref('dim_country') }} d
    on s.country = d.country
