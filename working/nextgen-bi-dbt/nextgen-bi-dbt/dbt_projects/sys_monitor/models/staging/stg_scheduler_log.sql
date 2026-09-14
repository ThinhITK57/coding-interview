select
  cast(scheduler_log_uuid as {{ dbt.type_string() }}) as scheduler_log_uuid,
  cast(task as {{ dbt.type_string() }}) as task,
  cast(scheduler_uuid as {{ dbt.type_string() }}) as scheduler_uuid,
  cast(job_id as {{ dbt.type_string() }}) as job_id,
  cast(created_at as timestamp) as created_at,
  cast(scheduled_time as timestamp) as scheduled_time,
  cast(job_group as {{ dbt.type_string() }}) as job_group,
  cast(status as {{ dbt.type_string() }}) as status,
  cast(target as {{ dbt.type_string() }}) as target,
  cast(target_type as {{ dbt.type_string() }}) as target_type
from {{ source('raw_lightdash_system', 'scheduler_log') }}
