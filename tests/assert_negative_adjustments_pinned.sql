-- Real source anomaly: a handful of ACCSP records carry negative pounds/dollars
-- (reconciliation adjustments). They are kept so totals match NOAA, and pinned here:
-- the build fails if new negatives appear outside ACCSP, or if any is material (> 5,000 lb or > $5,000).
select source_row_id, landing_year, state_name, species_name, data_source, landed_lbs, exvessel_usd
from {{ ref('stg_noaa__landings') }}
where (landed_lbs < 0 or exvessel_usd < 0)
  and (data_source <> 'ACCSP' or landed_lbs < -5000 or exvessel_usd < -5000)
