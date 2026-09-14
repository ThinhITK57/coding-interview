with bi_activity as (
    select
        viewed_date as event_date,
        user_uuid
    from {{ ref('stg_analytics_chart_views') }}

    union all

    select
        viewed_date as event_date,
        user_uuid
    from {{ ref('stg_analytics_dashboard_views') }}

    union all

    select
        viewed_date as event_date,
        user_uuid
    from {{ ref('stg_analytics_app_views') }}
),

ai_activity as (
    select
        cast(created_at as date) as event_date,
        created_by_user_uuid as user_uuid,
        ai_thread_uuid
    from {{ ref('stg_ai_prompts') }}
    where created_by_user_uuid is not null
)

select
    coalesce(b.event_date, a.event_date) as event_date,
    count(distinct b.user_uuid) as bi_active_users,
    count(distinct a.user_uuid) as ai_active_users,
    count(distinct a.ai_thread_uuid) as ai_thread_count
from bi_activity b
full outer join ai_activity a
    on a.event_date = b.event_date
    and a.user_uuid = b.user_uuid
group by 1
