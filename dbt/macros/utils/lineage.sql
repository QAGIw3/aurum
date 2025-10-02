{% macro aurum_lineage_append(existing_expr, addition_expr) %}
  (case
     when {{ existing_expr }} is null or {{ existing_expr }} = '' then {{ addition_expr }}
     else {{ existing_expr }} || '|' || {{ addition_expr }}
   end)
{% endmacro %}

