with companies as (
	select distinct
		trim(company) as company
	from {{ ref('stg_movie_companies') }}
	where company is not null
		and trim(company) <> ''
),
canonical as (
	select
		company,
		upper(company) as company_norm
	from companies
),
deduped as (
	-- One row per normalized company guarantees a true 1:* relationship to bridge tables.
	select
		company_norm,
		min(company) as company
	from canonical
	group by company_norm
)

select
	md5(company_norm) as company_key,
	company,
	company_norm
from deduped

