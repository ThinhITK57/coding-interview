with project_membership_access as (
    select
        cast(p.project_uuid as {{ dbt.type_string() }}) as project_uuid,
        cast(p.name as {{ dbt.type_string() }}) as project_name,
        cast(o.organization_uuid as {{ dbt.type_string() }}) as organization_uuid,
        cast(u.user_uuid as {{ dbt.type_string() }}) as user_uuid,
        cast(e.email as {{ dbt.type_string() }}) as user_email,
        cast(coalesce(pm.role, 'viewer') as {{ dbt.type_string() }}) as access_role,
        case coalesce(pm.role, 'viewer')
            when 'viewer' then 1
            when 'interactive_viewer' then 2
            when 'editor' then 3
            when 'developer' then 4
            when 'admin' then 5
            else 0
        end as access_role_rank,
        cast('project_membership' as {{ dbt.type_string() }}) as access_source,
        1 as access_source_rank
    from {{ source('raw_lightdash_system', 'project_memberships') }} pm
    inner join {{ source('raw_lightdash_system', 'projects') }} p
        on p.project_id = pm.project_id
    inner join {{ source('raw_lightdash_system', 'organizations') }} o
        on o.organization_id = p.organization_id
    inner join {{ source('raw_lightdash_system', 'users') }} u
        on u.user_id = pm.user_id
    left join {{ source('raw_lightdash_system', 'emails') }} e
        on e.user_id = u.user_id
        and e.is_primary = true
),
group_access as (
    select
        cast(p.project_uuid as {{ dbt.type_string() }}) as project_uuid,
        cast(p.name as {{ dbt.type_string() }}) as project_name,
        cast(o.organization_uuid as {{ dbt.type_string() }}) as organization_uuid,
        cast(u.user_uuid as {{ dbt.type_string() }}) as user_uuid,
        cast(e.email as {{ dbt.type_string() }}) as user_email,
        cast(coalesce(pga.role, 'viewer') as {{ dbt.type_string() }}) as access_role,
        case coalesce(pga.role, 'viewer')
            when 'viewer' then 1
            when 'interactive_viewer' then 2
            when 'editor' then 3
            when 'developer' then 4
            when 'admin' then 5
            else 0
        end as access_role_rank,
        cast('project_group_access' as {{ dbt.type_string() }}) as access_source,
        2 as access_source_rank
    from {{ source('raw_lightdash_system', 'project_group_access') }} pga
    inner join {{ source('raw_lightdash_system', 'projects') }} p
        on p.project_uuid = pga.project_uuid
    inner join {{ source('raw_lightdash_system', 'organizations') }} o
        on o.organization_id = p.organization_id
    inner join {{ source('raw_lightdash_system', 'group_memberships') }} gm
        on gm.group_uuid = pga.group_uuid
    inner join {{ source('raw_lightdash_system', 'users') }} u
        on u.user_id = gm.user_id
    left join {{ source('raw_lightdash_system', 'emails') }} e
        on e.user_id = u.user_id
        and e.is_primary = true
),
organization_access as (
    select
        cast(p.project_uuid as {{ dbt.type_string() }}) as project_uuid,
        cast(p.name as {{ dbt.type_string() }}) as project_name,
        cast(o.organization_uuid as {{ dbt.type_string() }}) as organization_uuid,
        cast(u.user_uuid as {{ dbt.type_string() }}) as user_uuid,
        cast(e.email as {{ dbt.type_string() }}) as user_email,
        cast(coalesce(om.role, 'viewer') as {{ dbt.type_string() }}) as access_role,
        case coalesce(om.role, 'viewer')
            when 'viewer' then 1
            when 'interactive_viewer' then 2
            when 'editor' then 3
            when 'developer' then 4
            when 'admin' then 5
            else 0
        end as access_role_rank,
        cast('organization_membership' as {{ dbt.type_string() }}) as access_source,
        3 as access_source_rank
    from {{ source('raw_lightdash_system', 'organization_memberships') }} om
    inner join {{ source('raw_lightdash_system', 'projects') }} p
        on p.organization_id = om.organization_id
    inner join {{ source('raw_lightdash_system', 'organizations') }} o
        on o.organization_id = om.organization_id
    inner join {{ source('raw_lightdash_system', 'users') }} u
        on u.user_id = om.user_id
    left join {{ source('raw_lightdash_system', 'emails') }} e
        on e.user_id = u.user_id
        and e.is_primary = true
    where om.role <> 'member'
),
unioned as (
    select * from project_membership_access
    union all
    select * from group_access
    union all
    select * from organization_access
),
deduped as (
    select
        *,
        row_number() over (
            partition by project_uuid, user_uuid
            order by access_role_rank desc, access_source_rank asc
        ) as rn
    from unioned
)
select
    project_uuid,
    project_name,
    organization_uuid,
    user_uuid,
    user_email,
    access_role,
    access_source
from deduped
where rn = 1
