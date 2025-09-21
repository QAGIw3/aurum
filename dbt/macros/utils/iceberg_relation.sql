{% macro iceberg_relation(schema_name, table_name, seed_name=None) %}
  {%- set seed_name = seed_name or table_name -%}
  {%- if target.name == 'duckdb' -%}
    {{ ref(seed_name) }}
  {%- else -%}
    {{ var('iceberg_catalog', 'iceberg') }}.{{ schema_name }}.{{ table_name }}
  {%- endif -%}
{% endmacro %}
