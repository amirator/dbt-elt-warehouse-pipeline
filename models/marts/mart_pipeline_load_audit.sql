-- Monitoring view of the EL job: latest status per partition and counts per run
with ranked as (
    select *,
           row_number() over (partition by landing_year order by finished_at desc) as rn
    from {{ ref('stg_noaa__load_log') }}
)

select
    landing_year,
    action          as last_action,
    rows_loaded     as last_rows_loaded,
    partition_sha256,
    finished_at     as last_attempt_at,
    (select count(distinct run_id) from {{ ref('stg_noaa__load_log') }}) as total_runs
from ranked
where rn = 1
