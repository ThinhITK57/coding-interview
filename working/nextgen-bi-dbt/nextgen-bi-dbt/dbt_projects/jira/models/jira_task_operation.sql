select *
from {{ source('bi_silver__jira', 'jira_task_operation') }}
