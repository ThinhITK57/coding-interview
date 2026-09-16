{{
    config(
        materialized='view',
        schema='bi_gold',
        alias='vw_br01_bsc_yearly',
        tags=['epm', 'gold', 'br01', 'bsc']
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
    cast(coalesce(t.c_target_date_m, t.target_date) as date) as target_date_m,
    cast(coalesce(t.c_target_value_m, t.target_value) as double) as target_value_m,
    cast(t.c_target_date_n as date) as target_date_n,
    cast(t.c_target_value_n as double) as target_value_n,
    t.state,
    t.status,
    coalesce(cast(t.c_target_result_value_m as double), try_cast(t.c_target_result_m as double)) as target_result_m,
    coalesce(cast(t.c_target_result_value_n as double), try_cast(t.c_target_result_n as double)) as target_result_n,
    coalesce(a.c_assignor, t.c_assignor, t.created_by) as assignor
from {{ source('bi_silver__epm', 'epm_targets') }} t
left join {{ source('bi_silver__epm', 'epm_c_assignments') }} a
    on t.c_associated_assignment = a.sysid
