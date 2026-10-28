-- Atlantic seafood relevant to a Maritime / Gulf of Maine operator: New England states, watch species groups
with base as (
    select
        f.landing_year,
        f.state_name,
        d.species_group,
        sum(f.landed_lbs)   as landed_lbs,
        sum(f.exvessel_usd) as exvessel_usd
    from {{ ref('fct_landings') }} f
    join {{ ref('dim_species') }} d on f.species_key = d.species_key
    where f.region_name = 'New England' and d.is_watch_group
    group by f.landing_year, f.state_name, d.species_group
)

select
    {{ surrogate_key(['landing_year', 'state_name', 'species_group']) }} as watch_key,
    *,
    {{ safe_divide('exvessel_usd', 'landed_lbs') }} as usd_per_lb
from base
