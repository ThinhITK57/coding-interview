select *
from {{ source('bi_silver__finance', 'finance_cost_plan') }}
