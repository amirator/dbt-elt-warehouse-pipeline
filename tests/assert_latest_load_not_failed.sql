-- Fails the build if the most recent attempt for any partition failed (pipeline monitoring)
select landing_year, action
from (
    select landing_year, action, row_number() over (partition by landing_year order by finished_at desc) as rn
    from {{ ref('stg_noaa__load_log') }}
) t
where rn = 1 and action = 'failed'
