select *
from {{ source('bi_silver__crm', 'crm_am_kpi_event_monthly') }}
