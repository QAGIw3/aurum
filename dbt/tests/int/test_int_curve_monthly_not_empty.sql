select count(*) as row_count
from {{ ref('int_curve_monthly') }}
having count(*) = 0
