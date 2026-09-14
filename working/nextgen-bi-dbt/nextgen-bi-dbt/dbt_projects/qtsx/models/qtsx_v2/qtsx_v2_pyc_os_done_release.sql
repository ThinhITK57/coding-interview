select *
from {{ source('pbi_silver__qtsx_v2', 'qtsx_v2_pyc_os_done_release') }}
