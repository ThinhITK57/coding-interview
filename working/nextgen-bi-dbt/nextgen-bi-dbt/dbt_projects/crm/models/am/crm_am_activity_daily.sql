select *
from {{ source('bi_silver__crm', 'crm_am_activity_daily') }}
