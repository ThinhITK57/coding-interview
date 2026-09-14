select *
from {{ source('bi_silver__cx', 'cx_cso_ticket_tags') }}
