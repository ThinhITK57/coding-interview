select *
from {{ source('pbi_silver__qtsx_v2', 'qtsx_v2_version_issue') }}
