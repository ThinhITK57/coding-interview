select *
from {{ source('bi_silver__crm', 'crm_mart_estimated_revenue') }}
