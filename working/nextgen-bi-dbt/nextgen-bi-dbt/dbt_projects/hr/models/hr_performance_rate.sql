select *
from {{ source('bi_silver__hr', 'hr_performance_rate') }}
