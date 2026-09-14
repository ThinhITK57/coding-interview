select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_mttd_mttr_miss_tan_cong') }}
