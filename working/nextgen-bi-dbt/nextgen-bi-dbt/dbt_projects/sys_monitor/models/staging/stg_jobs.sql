select
  cast(job_uuid as {{ dbt.type_string() }}) as job_uuid,
  cast(project_uuid as {{ dbt.type_string() }}) as project_uuid,
  cast(user_uuid as {{ dbt.type_string() }}) as user_uuid,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at,
  cast(job_status as {{ dbt.type_string() }}) as job_status,
  cast(job_type as {{ dbt.type_string() }}) as job_type
from {{ source('raw_lightdash_system', 'jobs') }}
