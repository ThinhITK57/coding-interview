select
  cast(user_id as integer) as user_id,
  cast(user_uuid as {{ dbt.type_string() }}) as user_uuid,
  cast(first_name as {{ dbt.type_string() }}) as first_name,
  cast(last_name as {{ dbt.type_string() }}) as last_name,
  cast(created_at as timestamp) as created_at,
  cast(is_marketing_opted_in as boolean) as is_marketing_opted_in,
  cast(is_tracking_anonymized as boolean) as is_tracking_anonymized,
  cast(is_setup_complete as boolean) as is_setup_complete,
  cast(is_active as boolean) as is_active,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_lightdash_system', 'users') }}
