select *
from {{ source('bi_silver__crm', 'crm_product_category') }}
