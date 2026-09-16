{{
    config(
        materialized='view',
        schema='bi_gold',
        alias='vw_project_report',
        tags=['epm', 'gold', 'projects', 'portfolio']
    )
}}

select
    p.name,
    coalesce(p.c_internal_type, p.project_type)                                             as project_type,
    cast(p.due_date as date)                                                                as due_date,
    p.state,
    coalesce(p.track_status, p.status)                                                      as status,
    cast(p.percent_completed as double)                                                     as percent_completed,
    p.c_department                                                                          as department,
    coalesce(p.c_department, 'Unknown')                                                     as department_name,
    coalesce(p.c_assignor, p.created_by)                                                    as assignor,
    coalesce(p.c_assignor, p.created_by, 'Unknown')                                         as assignor_name,
    p.c_assignee                                                                            as assignee,
    coalesce(p.c_assignee, 'Unknown')                                                       as assignee_name,
    coalesce(p.project_manager, p.manager)                                                  as project_manager,
    coalesce(p.project_manager, p.manager, 'Unknown')                                       as project_manager_name,
    coalesce(p.c_action_resources, cast(p.resources_and_placeholders_count as varchar))     as resources
from {{ source('bi_silver__epm', 'epm_projects') }} p
