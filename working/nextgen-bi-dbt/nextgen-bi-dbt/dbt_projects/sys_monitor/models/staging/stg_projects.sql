select
  cast(project_id as integer) as project_id,
  cast(project_uuid as {{ dbt.type_string() }}) as project_uuid,
  cast(name as {{ dbt.type_string() }}) as name,
  cast(project_type as {{ dbt.type_string() }}) as project_type,
  cast(created_at as timestamp) as created_at,
  cast(organization_id as integer) as organization_id,
  cast(dbt_connection_type as {{ dbt.type_string() }}) as dbt_connection_type,
  cast(created_by_user_uuid as {{ dbt.type_string() }}) as created_by_user_uuid,
  cast(dbt_version as {{ dbt.type_string() }}) as dbt_version,
  cast(scheduler_timezone as {{ dbt.type_string() }}) as scheduler_timezone,
  cast(query_timezone as {{ dbt.type_string() }}) as query_timezone,
  cast(has_default_user_spaces as boolean) as has_default_user_spaces,
  cast(project_defaults as {{ dbt.type_string() }}) as project_defaults,
  cast(color_palette_uuid as {{ dbt.type_string() }}) as color_palette_uuid
from {{ source('raw_lightdash_system', 'projects') }}
