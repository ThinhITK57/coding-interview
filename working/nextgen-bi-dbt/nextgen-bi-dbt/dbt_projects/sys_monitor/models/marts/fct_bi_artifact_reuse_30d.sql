with artifacts as (
    select
        'chart' as artifact_type,
        saved_query_uuid as artifact_uuid,
        name as artifact_name,
        created_at
    from {{ ref('stg_charts') }}
    where deleted_at is null

    union all

    select
        'dashboard' as artifact_type,
        dashboard_uuid as artifact_uuid,
        name as artifact_name,
        created_at
    from {{ ref('stg_dashboards') }}
    where deleted_at is null
),

views as (
    select
        'chart' as artifact_type,
        chart_uuid as artifact_uuid,
        viewed_at
    from {{ ref('stg_analytics_chart_views') }}

    union all

    select
        'dashboard' as artifact_type,
        dashboard_uuid as artifact_uuid,
        viewed_at
    from {{ ref('stg_analytics_dashboard_views') }}
)

select
    a.artifact_type,
    a.artifact_uuid,
    a.artifact_name,
    a.created_at,
    count(v.viewed_at) as views_after_30d,
    case when count(v.viewed_at) > 0 then true else false end as reused_after_30d
from artifacts a
left join views v
    on v.artifact_type = a.artifact_type
    and v.artifact_uuid = a.artifact_uuid
    and v.viewed_at >= a.created_at + interval '30 days'
group by 1, 2, 3, 4
