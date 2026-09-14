select
  cast(ai_agent_tool_result_uuid as {{ dbt.type_string() }}) as ai_agent_tool_result_uuid,
  cast(ai_prompt_uuid as {{ dbt.type_string() }}) as ai_prompt_uuid,
  cast(tool_call_id as {{ dbt.type_string() }}) as tool_call_id,
  cast(tool_name as {{ dbt.type_string() }}) as tool_name,
  cast(result as {{ dbt.type_string() }}) as result,
  cast(created_at as timestamp) as created_at
from {{ source('raw_lightdash_system', 'ai_agent_tool_result') }}
