{% macro aurum_text_hash(expression) %}
  {%- if target.name == 'duckdb' -%}
    lower(md5({{ expression }}))
  {%- else -%}
    lower(to_hex(md5(to_utf8({{ expression }}))))
  {%- endif -%}
{% endmacro %}
