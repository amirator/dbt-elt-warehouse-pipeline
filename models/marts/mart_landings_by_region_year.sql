select
    landing_year,
    region_name,
    sum(landed_lbs)                 as landed_lbs,
    sum(exvessel_usd)               as exvessel_usd,
    {{ safe_divide('sum(exvessel_usd)', 'sum(landed_lbs)') }} as usd_per_lb,
    count(*)                        as landing_rows
from {{ ref('fct_landings') }}
group by landing_year, region_name
