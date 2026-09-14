select *
from {{ source('bi_silver__finance', 'finance_daily_business_metrics') }}
