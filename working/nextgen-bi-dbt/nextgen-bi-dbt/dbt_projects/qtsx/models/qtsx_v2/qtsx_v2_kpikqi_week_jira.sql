select *
from {{ source('pbi_silver__qtsx_v2', 'qtsx_v2_kpikqi_week_jira') }}
