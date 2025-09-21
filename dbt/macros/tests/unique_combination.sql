{% test unique_combination(model, combination_of_columns, where=None) %}
  {% if combination_of_columns is undefined or combination_of_columns | length == 0 %}
    {%- do exceptions.raise_compiler_error('unique_combination requires at least one column') -%}
  {% endif %}

  {% set select_columns = combination_of_columns | join(', ') %}

  with records as (
      select
          {{ select_columns }}
      from {{ model }}
      {% if where %}
      where {{ where }}
      {% endif %}
  ), duplicates as (
      select
          {{ select_columns }},
          count(*) as record_count
      from records
      group by {{ select_columns }}
      having count(*) > 1
  )
  select * from duplicates
{% endtest %}
