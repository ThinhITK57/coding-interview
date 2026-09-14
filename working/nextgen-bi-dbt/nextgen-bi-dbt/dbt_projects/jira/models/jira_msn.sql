select *
from {{ source('bi_silver__jira', 'jira_msn') }}
