select *
from {{ source('bi_silver__cx', 'cx_cso_customer_contract_lifecycle_snapshot') }}
