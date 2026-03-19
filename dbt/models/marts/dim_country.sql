with countries as (
    select distinct
        trim(country) as country
    from {{ ref('stg_movie_countries') }}
    where country is not null
        and trim(country) <> ''
)

select
    dense_rank() over (order by country) as country_key,
    country
from countries
