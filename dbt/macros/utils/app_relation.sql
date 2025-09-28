{% macro app_relation(table_name, seed_name=None) %}
  {%- set seed_name = seed_name or table_name -%}
  {%- if target.name == 'duckdb' -%}
    {{ ref(seed_name) }}
  {%- else -%}
    {{ source('app_postgres', table_name) }}
  {%- endif -%}
{% endmacro %}
