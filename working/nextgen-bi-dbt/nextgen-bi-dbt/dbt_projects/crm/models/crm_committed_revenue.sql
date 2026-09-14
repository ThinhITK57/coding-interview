select *
from {{ source('bi_silver__crm', 'crm_committed_revenue') }}
