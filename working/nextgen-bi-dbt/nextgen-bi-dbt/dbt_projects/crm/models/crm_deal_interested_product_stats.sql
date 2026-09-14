select *
from {{ source('bi_silver__crm', 'crm_deal_interested_product_stats') }}
