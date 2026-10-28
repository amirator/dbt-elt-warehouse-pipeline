{# NOAA AFS names carry trailing blanks and a ' **' suffix marking grouped (not species-specific) records. #}
{% macro clean_species_name(col) -%}
    trim(replace({{ col }}, '**', ''))
{%- endmacro %}
