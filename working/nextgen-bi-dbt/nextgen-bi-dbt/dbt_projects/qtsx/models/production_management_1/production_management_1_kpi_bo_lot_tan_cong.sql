select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_kpi_bo_lot_tan_cong') }}
