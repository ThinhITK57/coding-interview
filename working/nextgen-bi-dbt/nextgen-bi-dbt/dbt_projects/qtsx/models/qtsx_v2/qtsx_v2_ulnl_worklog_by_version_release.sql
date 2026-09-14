select *
from {{ source('pbi_silver__qtsx_v2', 'qtsx_v2_ulnl_worklog_by_version_release') }}
