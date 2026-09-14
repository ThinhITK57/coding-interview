select
    viewed_date,
    app_id,
    count(*) as app_views,
    count(distinct user_uuid) as unique_viewers
from {{ ref('stg_analytics_app_views') }}
group by 1, 2
