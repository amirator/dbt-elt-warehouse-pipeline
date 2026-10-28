with species as (
    select
        {{ surrogate_key(['tsn', 'species_name']) }} as species_key,
        tsn,
        species_name,
        max(scientific_name)    as scientific_name,
        max(case when is_grouped_record then 1 else 0 end) = 1 as is_grouped_record,
        max(case when is_confidential then 1 else 0 end) = 1   as is_confidential
    from {{ ref('stg_noaa__landings') }}
    group by tsn, species_name
),

mapped as (
    select
        s.*,
        coalesce(m.species_group, case when s.is_confidential then 'Confidential' else 'Other' end) as species_group
    from species s
    left join {{ ref('species_group_map') }} m
        on upper(trim(split_part(coalesce(s.species_name, ''), ',', 1))) = m.name_prefix
)

select
    *,
    species_group in ({% for g in var('watch_groups') %}'{{ g }}'{% if not loop.last %}, {% endif %}{% endfor %}) as is_watch_group
from mapped
