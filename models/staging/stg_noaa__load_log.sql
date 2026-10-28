select
    run_id,
    cast(year as integer) as landing_year,
    action,
    rows_loaded,
    partition_sha256,
    source_url,
    started_at,
    finished_at,
    message
from {{ source('noaa_foss', '_load_log') }}
