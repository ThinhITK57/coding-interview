select
  cast(project_uuid as {{ dbt.type_string() }}) as project_uuid,
  cast(enabled as boolean) as enabled,
  cast(schedule_cron as {{ dbt.type_string() }}) as schedule_cron,
  cast(enabled_by_user_uuid as {{ dbt.type_string() }}) as enabled_by_user_uuid,
  cast(slack_channel_id as {{ dbt.type_string() }}) as slack_channel_id,
  cast(anthropic_agent_id as {{ dbt.type_string() }}) as anthropic_agent_id,
  cast(anthropic_agent_config_hash as {{ dbt.type_string() }}) as anthropic_agent_config_hash,
  cast(anthropic_environment_id as {{ dbt.type_string() }}) as anthropic_environment_id,
  cast(anthropic_vault_id as {{ dbt.type_string() }}) as anthropic_vault_id,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_lightdash_system', 'managed_agent_settings') }}
