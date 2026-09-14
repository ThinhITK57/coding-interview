select *
from {{ source('bi_silver__crm', 'crm_revenue_reconciliation') }}
