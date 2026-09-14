select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_mm_os_qi_2026') }}
