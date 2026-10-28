{{
    config(
        materialized='incremental',
        unique_key='landing_key',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}
{# Incremental: on each run only the trailing `lookback_years` (relative to the
   current max year in the target) plus any newer years are re-processed,
   because NOAA revises recent landings. delete+insert works on DuckDB and Snowflake. #}
select
    a.landing_key,
    a.species_key,
    a.landing_year,
    a.state_name,
    a.region_name,
    a.data_source,
    a.source_record_count,
    a.landed_lbs,
    a.exvessel_usd,
    {{ safe_divide('a.exvessel_usd', 'a.landed_lbs') }} as usd_per_lb,
    a.loaded_at
from {{ ref('int_landings__aggregated') }} a
{% if is_incremental() %}
where a.landing_year >= (select max(landing_year) - {{ var('lookback_years') }} + 1 from {{ this }})
{% endif %}
