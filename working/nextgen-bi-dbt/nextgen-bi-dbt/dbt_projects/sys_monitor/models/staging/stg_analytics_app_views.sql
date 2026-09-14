select
  cast(analytics_app_view_uuid as {{ dbt.type_string() }}) as analytics_app_view_uuid,
  cast(app_id as {{ dbt.type_string() }}) as app_id,
  cast(user_uuid as {{ dbt.type_string() }}) as user_uuid,
  cast(timestamp as timestamp) as timestamp,
  cast(timestamp as timestamp) as viewed_at,
  cast(timestamp as date) as viewed_date
from {{ source('raw_lightdash_system', 'analytics_app_views') }}
