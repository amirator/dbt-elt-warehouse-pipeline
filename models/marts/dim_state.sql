select
    state_name,
    max(region_name) as region_name,
    min(landing_year) as first_year,
    max(landing_year) as last_year
from {{ ref('fct_landings') }}
group by state_name
