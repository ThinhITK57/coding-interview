select
  cast(t.ai_thread_uuid as {{ dbt.type_string() }}) as ai_thread_uuid,
  cast(t.agent_uuid as {{ dbt.type_string() }}) as agent_uuid,
  cast(a.name as {{ dbt.type_string() }}) as agent_name,
  cast(t.organization_uuid as {{ dbt.type_string() }}) as organization_uuid,
  cast(t.project_uuid as {{ dbt.type_string() }}) as project_uuid,
  cast(t.created_from as {{ dbt.type_string() }}) as created_from,
  cast(t.title as {{ dbt.type_string() }}) as title,
  cast(t.title_generated_at as timestamp) as title_generated_at,
  cast(t.created_at as timestamp) as created_at
from {{ source('raw_lightdash_system', 'ai_thread') }} t
left join {{ source('raw_lightdash_system', 'ai_agent') }} a
  on a.ai_agent_uuid = t.agent_uuid
