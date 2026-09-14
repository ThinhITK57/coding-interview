with dashboard_usage as (
    select
        viewed_date,
        dashboard_uuid,
        count(*) as dashboard_views,
        count(distinct user_uuid) as unique_viewers
    from {{ ref('stg_analytics_dashboard_views') }}
    group by 1, 2
)

select
    u.viewed_date,
    u.dashboard_uuid,
    d.name as dashboard_name,
    u.dashboard_views,
    u.unique_viewers
from dashboard_usage u
left join {{ ref('stg_dashboards') }} d
    on d.dashboard_uuid = u.dashboard_uuid
