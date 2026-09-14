select
  cast(dashboard_id as integer) as dashboard_id,
  cast(dashboard_uuid as {{ dbt.type_string() }}) as dashboard_uuid,
  cast(name as {{ dbt.type_string() }}) as name,
  cast(description as {{ dbt.type_string() }}) as description,
  cast(space_id as integer) as space_id,
  cast(created_at as timestamp) as created_at,
  cast(slug as {{ dbt.type_string() }}) as slug,
  cast(views_count as integer) as views_count,
  cast(first_viewed_at as timestamp) as first_viewed_at,
  cast(deleted_at as timestamp) as deleted_at,
  cast(deleted_by_user_uuid as {{ dbt.type_string() }}) as deleted_by_user_uuid
from {{ source('raw_lightdash_system', 'dashboards') }}
