with source as (
    select * from {{ source('noaa_foss', 'foss_landings') }}
),

renamed as (
    select
        source_row_id,
        cast(year as integer)                              as landing_year,
        upper(trim(state_name))                            as state_name,
        trim(region_name)                                  as region_name,
        nullif(trim(tsn), '')                              as tsn,
        {{ clean_species_name('ts_afs_name') }}            as species_name,
        nullif(trim(ts_scientific_name), '')               as scientific_name,
        ts_afs_name like '%**%'                            as is_grouped_record,
        upper(trim(ts_afs_name)) = 'WITHHELD FOR CONFIDENTIALITY' as is_confidential,
        trim(source)                                       as data_source,
        cast(pounds as double)                             as landed_lbs,
        cast(dollars as double)                            as exvessel_usd,
        _batch_id                                          as batch_id,
        _loaded_at                                         as loaded_at
    from source
)

select * from renamed
