-- Totals per year in the fact must equal the raw layer (no rows lost or double counted by aggregation/incremental logic)
with raw_totals as (
    select year as landing_year, sum(coalesce(dollars, 0)) as usd, sum(coalesce(pounds, 0)) as lbs
    from {{ source('noaa_foss', 'foss_landings') }} group by year
),
fct_totals as (
    select landing_year, sum(coalesce(exvessel_usd, 0)) as usd, sum(coalesce(landed_lbs, 0)) as lbs
    from {{ ref('fct_landings') }} group by landing_year
)
select r.landing_year, r.usd, f.usd as fct_usd, r.lbs, f.lbs as fct_lbs
from raw_totals r
full outer join fct_totals f on r.landing_year = f.landing_year
where f.landing_year is null or r.landing_year is null
   or abs(r.usd - f.usd) > 1 or abs(r.lbs - f.lbs) > 1
