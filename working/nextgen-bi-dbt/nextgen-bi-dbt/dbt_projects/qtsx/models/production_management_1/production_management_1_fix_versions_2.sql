select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_fix_versions_2') }}
