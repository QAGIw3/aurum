{% macro aurum_reference_strips(start_date=None, end_date=None, asof_relation=None) %}
  {%- set rel = asof_relation or ref('dim_asof') -%}
  {%- set filters = [] -%}
  {%- if start_date -%}
    {%- do filters.append("asof_date >= cast('" ~ start_date ~ "' as date)") -%}
  {%- endif -%}
  {%- if end_date -%}
    {%- do filters.append("asof_date <= cast('" ~ end_date ~ "' as date)") -%}
  {%- endif -%}

  with base as (
      select distinct
          asof_date,
          date_trunc('month', asof_date) as month_start,
          date_trunc('week', asof_date) as week_start
      from {{ rel }}
      {%- if filters %}
      where {{ filters | join(' and ') }}
      {%- endif %}
  ), calendar_strips as (
      select distinct
          'CALENDAR' as strip_type,
          month_start as strip_start,
          date_add('day', -1, date_add('month', 1, month_start)) as strip_end,
          month_start as anchor_date,
          '7X24' as block_code
      from base
  ), weekly_strips as (
      select distinct
          'WEEKLY' as strip_type,
          week_start as strip_start,
          date_add('day', 6, week_start) as strip_end,
          week_start as anchor_date,
          '7X24' as block_code
      from base
  ), weekly_peak as (
      select distinct
          'WEEKLY_PEAK' as strip_type,
          week_start as strip_start,
          date_add('day', 6, week_start) as strip_end,
          week_start as anchor_date,
          '5X16' as block_code
      from base
  ), weekly_offpeak as (
      select distinct
          'WEEKLY_OFFPEAK' as strip_type,
          week_start as strip_start,
          date_add('day', 6, week_start) as strip_end,
          week_start as anchor_date,
          'OFFPEAK' as block_code
      from base
  )
  select * from calendar_strips
  union all
  select * from weekly_strips
  union all
  select * from weekly_peak
  union all
  select * from weekly_offpeak
{% endmacro %}
