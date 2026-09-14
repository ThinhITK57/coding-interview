select
  cast(action_uuid as {{ dbt.type_string() }}) as action_uuid,
  cast(project_uuid as {{ dbt.type_string() }}) as project_uuid,
  cast(session_id as {{ dbt.type_string() }}) as session_id,
  cast(action_type as {{ dbt.type_string() }}) as action_type,
  cast(target_type as {{ dbt.type_string() }}) as target_type,
  cast(target_uuid as {{ dbt.type_string() }}) as target_uuid,
  cast(target_name as {{ dbt.type_string() }}) as target_name,
  cast(description as {{ dbt.type_string() }}) as description,
  cast(reversed_at as timestamp) as reversed_at,
  cast(reversed_by_user_uuid as {{ dbt.type_string() }}) as reversed_by_user_uuid,
  cast(created_at as timestamp) as created_at
from {{ source('raw_lightdash_system', 'managed_agent_actions') }}
