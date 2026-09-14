select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_n_5_vvip_prm_cause_group') }}
