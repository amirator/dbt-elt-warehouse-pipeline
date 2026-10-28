with yearly as (
    select
        f.landing_year,
        d.species_group,
        max(case when d.is_watch_group then 1 else 0 end) = 1 as is_watch_group,
        sum(f.landed_lbs)   as landed_lbs,
        sum(f.exvessel_usd) as exvessel_usd
    from {{ ref('fct_landings') }} f
    join {{ ref('dim_species') }} d on f.species_key = d.species_key
    group by f.landing_year, d.species_group
)

select
    landing_year,
    species_group,
    is_watch_group,
    landed_lbs,
    exvessel_usd,
    {{ safe_divide('exvessel_usd', 'landed_lbs') }} as usd_per_lb,
    {{ safe_divide('exvessel_usd', 'sum(exvessel_usd) over (partition by landing_year)') }} as share_of_year_usd,
    {{ safe_divide('exvessel_usd - lag(exvessel_usd) over (partition by species_group order by landing_year)',
                   'lag(exvessel_usd) over (partition by species_group order by landing_year)') }} as yoy_usd_change,
    avg(exvessel_usd) over (partition by species_group order by landing_year
                            rows between 4 preceding and current row) as usd_5yr_rolling_avg
from yearly
