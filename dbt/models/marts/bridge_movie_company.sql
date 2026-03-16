with src as (
  select
    movie_id,
    trim(company) as company,
    upper(trim(company)) as company_norm
  from {{ ref('stg_movie_companies') }}
  where company is not null
    and trim(company) <> ''
)

select distinct
  s.movie_id,
  d.company_key,
  d.company,
  d.company_norm
from src s
join {{ ref('dim_company') }} d
  on s.company_norm = d.company_norm
