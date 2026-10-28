{# Equivalent of dbt_utils.unique_combination_of_columns, kept local (no package install needed). #}
{% test dbt_utils_free_unique_combo(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
