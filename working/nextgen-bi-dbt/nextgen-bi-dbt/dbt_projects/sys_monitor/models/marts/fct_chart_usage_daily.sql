with chart_usage as (
    select
        viewed_date,
        chart_uuid,
        count(*) as chart_views,
        count(distinct user_uuid) as unique_viewers
    from {{ ref('stg_analytics_chart_views') }}
    group by 1, 2
)

select
    u.viewed_date,
    u.chart_uuid,
    c.name as chart_name,
    u.chart_views,
    u.unique_viewers
from chart_usage u
left join {{ ref('stg_charts') }} c
    on c.saved_query_uuid = u.chart_uuid
