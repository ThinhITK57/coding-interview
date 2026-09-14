select *
from {{ source('bi_silver__crm', 'crm_deals') }}
