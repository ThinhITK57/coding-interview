select *
from {{ source('bi_silver__crm', 'crm_deal_reason_stats') }}
