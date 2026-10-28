select species_key, tsn, species_name, scientific_name, species_group,
       is_watch_group, is_grouped_record, is_confidential
from {{ ref('int_species__classified') }}
