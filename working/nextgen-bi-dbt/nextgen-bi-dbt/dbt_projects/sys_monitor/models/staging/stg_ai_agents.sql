select
  cast(ai_agent_uuid as {{ dbt.type_string() }}) as ai_agent_uuid,
  cast(organization_uuid as {{ dbt.type_string() }}) as organization_uuid,
  cast(project_uuid as {{ dbt.type_string() }}) as project_uuid,
  cast(name as {{ dbt.type_string() }}) as name,
  cast(slug as {{ dbt.type_string() }}) as slug,
  cast(description as {{ dbt.type_string() }}) as description,
  cast(image_url as {{ dbt.type_string() }}) as image_url,
  cast(enable_data_access as boolean) as enable_data_access,
  cast(enable_self_improvement as boolean) as enable_self_improvement,
  cast(enable_reasoning as boolean) as enable_reasoning,
  cast(version as integer) as version,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_lightdash_system', 'ai_agent') }}
