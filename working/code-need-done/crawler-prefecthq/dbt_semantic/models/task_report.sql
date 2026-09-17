{{
    config(
        materialized='view',
        schema='bi_gold',
        alias='vw_task_report',
        tags=['epm', 'gold', 'tasks']
    )
}}

select
    t.task_type,
    coalesce(t.manager, t.c_assignee) as assignee,
    t.c_department as department,
    cast(coalesce(t.all_user_resources_count, t.resources_and_placeholders_count) as varchar) as resources,
    t.parent_project,
    coalesce(t.track_status, t.status) as jira_status,
    t.name,
    t.description,
    cast(t.work as double) as work,
    cast(t.duration as double) as duration,
    coalesce(t.c_epm_default, t.default_integration_path) as epm_default,
    cast(t.start_date as date) as start_date,
    cast(t.due_date as date) as due_date,
    cast(t.percent_completed as double) as percent_completed,
    coalesce(t.c_update_description, t.overview) as update_description
from {{ source('bi_silver__epm', 'epm_tasks') }} t