select *
from {{ source('bi_silver__finance', 'finance_sales_revenue') }}
