{{
    config(
        materialized='view',
        schema='bi_gold',
        alias='vw_dieu_hanh_cvct_klcd',
        tags=['epm', 'gold', 'executive']
    )
}}

select
    coalesce(t.c_assignee, t.entity_owner) as assignee,
    cast(t.resources_and_placeholders_count as varchar) as resources,
    t.name,
    t.status,
    cast(coalesce(t.c_target_date_m, t.target_date) as date) as due_date,
    cast(t.percent_completed as double) as percent_completed,
    cast(coalesce(t.c_target_value_m, t.target_value) as double) as target_value_m,
    cast(coalesce(t.c_target_date_m, t.target_date) as date) as target_date_m
from {{ source('bi_silver__epm', 'epm_targets') }} t