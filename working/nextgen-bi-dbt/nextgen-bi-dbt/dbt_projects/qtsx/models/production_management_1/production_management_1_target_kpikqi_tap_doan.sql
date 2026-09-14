select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_target_kpikqi_tap_doan') }}
