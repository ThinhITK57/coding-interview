select
  cast(step_id as integer) as step_id,
  cast(job_uuid as {{ dbt.type_string() }}) as job_uuid,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at,
  cast(step_status as {{ dbt.type_string() }}) as step_status,
  cast(step_type as {{ dbt.type_string() }}) as step_type,
  cast(step_error as {{ dbt.type_string() }}) as step_error,
  cast(started_at as timestamp) as started_at
from {{ source('raw_lightdash_system', 'job_steps') }}
