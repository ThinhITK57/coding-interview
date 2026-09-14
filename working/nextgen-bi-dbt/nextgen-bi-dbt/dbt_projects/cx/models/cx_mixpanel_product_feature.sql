select *
from {{ source('bi_silver__cx', 'cx_mixpanel_product_feature') }}
