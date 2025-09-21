select count(*) as row_count
from {{ ref('stg_eia_series') }}
having count(*) = 0
