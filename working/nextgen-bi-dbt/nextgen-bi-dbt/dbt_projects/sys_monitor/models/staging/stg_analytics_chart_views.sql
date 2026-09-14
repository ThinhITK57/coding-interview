select
  cast(analytics_chart_view_uuid as {{ dbt.type_string() }}) as analytics_chart_view_uuid,
  cast(chart_uuid as {{ dbt.type_string() }}) as chart_uuid,
  cast(user_uuid as {{ dbt.type_string() }}) as user_uuid,
  cast(timestamp as timestamp) as timestamp,
  cast(timestamp as timestamp) as viewed_at,
  cast(timestamp as date) as viewed_date,
  cast(context as {{ dbt.type_string() }}) as context
from {{ source('raw_lightdash_system', 'analytics_chart_views') }}
