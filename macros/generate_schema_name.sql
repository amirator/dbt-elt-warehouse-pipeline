{# Use the custom schema name as-is (staging, intermediate, marts) instead of <target>_<custom>. Pattern from dbt-labs/jaffle-shop. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
