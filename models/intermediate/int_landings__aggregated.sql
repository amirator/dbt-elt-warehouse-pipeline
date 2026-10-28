{# FOSS returns several sub-records for the same state/species/source/year
   (e.g. 42 confidential rows for Hawaii 2024, two rows for Oregon Pacific ocean perch).
   Collapse them to one row per business grain. #}
with stg as (
    select * from {{ ref('stg_noaa__landings') }}
),

state_region as (
    -- 15 raw rows have no region; fill from the state's most frequent region
    select state_name, region_name
    from (
        select state_name, region_name,
               row_number() over (partition by state_name order by count(*) desc, region_name) as rn
        from stg
        where region_name is not null
        group by state_name, region_name
    ) ranked
    where rn = 1
)

select
    {{ surrogate_key(['s.landing_year', 's.state_name', 's.tsn', 's.species_name', 's.data_source']) }} as landing_key,
    {{ surrogate_key(['s.tsn', 's.species_name']) }}                                                    as species_key,
    s.landing_year,
    s.state_name,
    coalesce(max(s.region_name), max(r.region_name), 'Unknown')  as region_name,
    s.tsn,
    s.species_name,
    s.data_source,
    count(*)                                  as source_record_count,
    sum(s.landed_lbs)                         as landed_lbs,
    sum(s.exvessel_usd)                       as exvessel_usd,
    max(s.loaded_at)                          as loaded_at
from stg s
left join state_region r on s.state_name = r.state_name
group by s.landing_year, s.state_name, s.tsn, s.species_name, s.data_source
