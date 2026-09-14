{{
    config(
        materialized='view',
        schema='bi_gold',
        alias='vw_br05_user_access_traffic',
        tags=['epm', 'gold', 'br05', 'access_log']
    )
}}

select
    cast(u.login_date as date) as login_date,
    u.name,
    u.first_name,
    u.last_name,
    u.groups,
    u.direct_manager,
    u.job_title
from {{ source('bi_silver__epm', 'epm_user_access_log') }} u
