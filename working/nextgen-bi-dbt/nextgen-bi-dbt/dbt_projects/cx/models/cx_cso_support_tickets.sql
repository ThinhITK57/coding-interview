select *
from {{ source('bi_silver__cx', 'cx_cso_support_tickets') }}
