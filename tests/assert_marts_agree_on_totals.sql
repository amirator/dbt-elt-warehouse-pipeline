-- Region mart and species-group mart are two cuts of the same fact: yearly totals must match
with r as (select landing_year, sum(exvessel_usd) as usd from {{ ref('mart_landings_by_region_year') }} group by landing_year),
     s as (select landing_year, sum(exvessel_usd) as usd from {{ ref('mart_species_group_trends') }} group by landing_year)
select r.landing_year, r.usd, s.usd as species_usd
from r join s on r.landing_year = s.landing_year
where abs(coalesce(r.usd, 0) - coalesce(s.usd, 0)) > 1
