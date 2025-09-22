select *
from {{ ref('fct_curve_observation') }}
where mid is null
