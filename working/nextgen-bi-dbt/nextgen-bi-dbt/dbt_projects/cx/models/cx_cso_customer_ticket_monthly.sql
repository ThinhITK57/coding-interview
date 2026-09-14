select *
from {{ source('bi_silver__cx', 'cx_cso_customer_ticket_monthly') }}
