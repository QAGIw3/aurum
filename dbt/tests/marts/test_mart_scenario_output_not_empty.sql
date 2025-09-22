select count(*) as row_count
from {{ ref('mart_scenario_output') }}
having count(*) = 0
