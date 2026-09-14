select *
from {{ source('pbi_silver__qtsx_v2', 'qtsx_v2_man_month_qa') }}
