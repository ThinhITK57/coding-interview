select *
from {{ source('bi_silver__crm', 'vw_crm_quotation_activity') }}
