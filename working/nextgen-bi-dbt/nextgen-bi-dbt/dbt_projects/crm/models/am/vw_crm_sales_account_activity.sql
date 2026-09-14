select *
from {{ source('bi_silver__crm', 'vw_crm_sales_account_activity') }}
