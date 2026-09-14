select *
from {{ source('bi_silver__cx', 'cx_lifecycle_customer_product_snapshot') }}
