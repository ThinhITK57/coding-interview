select *
from {{ source('bi_silver__hr', 'hr_workforce_monthly_drilldown') }}
