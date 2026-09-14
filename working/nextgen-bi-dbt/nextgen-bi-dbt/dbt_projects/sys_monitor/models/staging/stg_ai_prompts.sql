select
  cast(ai_prompt_uuid as {{ dbt.type_string() }}) as ai_prompt_uuid,
  cast(ai_thread_uuid as {{ dbt.type_string() }}) as ai_thread_uuid,
  cast(created_by_user_uuid as {{ dbt.type_string() }}) as created_by_user_uuid,
  cast(prompt as {{ dbt.type_string() }}) as prompt,
  cast(response as {{ dbt.type_string() }}) as response,
  cast(error_message as {{ dbt.type_string() }}) as error_message,
  cast(status as {{ dbt.type_string() }}) as status,
  cast(responded_at as timestamp) as responded_at,
  case
    when responded_at is not null
      then extract(epoch from (cast(responded_at as timestamp) - cast(created_at as timestamp)))
  end as response_time_seconds,
  case
    when responded_at is not null
      then extract(epoch from (cast(responded_at as timestamp) - cast(created_at as timestamp))) / 60.0
  end as response_time_minutes,
  cast(human_score as numeric) as human_score,
  cast(human_feedback as {{ dbt.type_string() }}) as human_feedback,
  cast(saved_query_uuid as {{ dbt.type_string() }}) as saved_query_uuid,
  cast(created_at as timestamp) as created_at
from {{ source('raw_lightdash_system', 'ai_prompt') }}
