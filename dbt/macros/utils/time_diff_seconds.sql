{% macro time_diff_seconds(start_expr, end_expr) %}
  {%- if target.name == 'duckdb' -%}
    datediff('second', {{ start_expr }}, {{ end_expr }})
  {%- else -%}
    date_diff('second', {{ start_expr }}, {{ end_expr }})
  {%- endif -%}
{% endmacro %}
