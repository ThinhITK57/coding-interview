select *
from {{ source('bi_silver__crm', 'vw_crm_contract_activity') }}
