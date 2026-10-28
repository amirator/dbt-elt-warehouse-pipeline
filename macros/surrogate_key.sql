{# Portable md5 surrogate key (DuckDB + Snowflake) built on dbt's cross-database macros.
   Same idea as dbt_utils.generate_surrogate_key, written locally to avoid a package dependency. #}
{% macro surrogate_key(columns) -%}
    {%- set parts = [] -%}
    {%- for c in columns -%}
        {%- do parts.append("coalesce(cast(" ~ c ~ " as " ~ dbt.type_string() ~ "), '_null_')") -%}
        {%- if not loop.last %}{%- do parts.append("'|'") -%}{% endif -%}
    {%- endfor -%}
    {{ dbt.hash(dbt.concat(parts)) }}
{%- endmacro %}
