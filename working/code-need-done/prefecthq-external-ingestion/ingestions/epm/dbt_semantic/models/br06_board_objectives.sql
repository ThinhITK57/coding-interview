{{
    config(
        materialized='view',
        schema='bi_gold',
        alias='vw_br06_board_objectives',
        tags=['epm', 'gold', 'br06', 'board']
    )
}}

select
    t.associated_objective,
    t.associated_item,
    t.target_type,
    t.parent_target,
    t.c_department,
    t.name,
    t.c_assignee,
    t.unit,
    cast(t.c_target_date_m as date) as c_target_date_m,
    cast(t.c_target_date_n as date) as c_target_date_n,
    cast(t.c_target_value_m as double) as c_target_value_m,
    cast(t.c_target_value_n as double) as c_target_value_n,
    coalesce(cast(t.c_target_result_value_m as double), try_cast(t.c_target_result_m as double)) as c_target_result_m,
    coalesce(cast(t.c_target_result_value_n as double), try_cast(t.c_target_result_n as double)) as c_target_result_n,
    t.state,
    t.status,
    coalesce(a.c_assignor, t.c_assignor, t.created_by) as c_assignor
from {{ source('bi_silver__epm', 'epm_targets') }} t
left join {{ source('bi_silver__epm', 'epm_assignments') }} a
    on t.c_associated_assignment = a.sysid
