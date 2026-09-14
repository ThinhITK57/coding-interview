select *
from {{ source('bi_silver__cx', 'cx_cso_mart_daily_ticket_summary') }}
