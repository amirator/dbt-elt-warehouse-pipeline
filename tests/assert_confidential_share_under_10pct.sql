-- Data-quality guardrail: confidentiality-withheld value stays a minor share of each year (observed max is pinned in README)
select landing_year, share_of_year_usd
from {{ ref('mart_species_group_trends') }}
where species_group = 'Confidential' and share_of_year_usd > 0.10
