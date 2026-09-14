select *
from {{ source('bi_silver__crm', 'crm_sales_accounts') }}
