select *
from {{ source('bi_silver__finance', 'finance_revenue_plan') }}
