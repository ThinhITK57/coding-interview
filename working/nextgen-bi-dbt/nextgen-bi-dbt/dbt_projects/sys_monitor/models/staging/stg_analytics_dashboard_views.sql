select
  cast(analytics_dashboard_view_uuid as {{ dbt.type_string() }}) as analytics_dashboard_view_uuid,
  cast(dashboard_uuid as {{ dbt.type_string() }}) as dashboard_uuid,
  cast(user_uuid as {{ dbt.type_string() }}) as user_uuid,
  cast(timestamp as timestamp) as timestamp,
  cast(timestamp as timestamp) as viewed_at,
  cast(timestamp as date) as viewed_date,
  cast(context as {{ dbt.type_string() }}) as context
from {{ source('raw_lightdash_system', 'analytics_dashboard_views') }}
