select
  cast(saved_query_id as integer) as saved_query_id,
  cast(saved_query_uuid as {{ dbt.type_string() }}) as saved_query_uuid,
  cast(name as {{ dbt.type_string() }}) as name,
  cast(description as {{ dbt.type_string() }}) as description,
  cast(space_id as integer) as space_id,
  cast(created_at as timestamp) as created_at,
  cast(deleted_at as timestamp) as deleted_at
from {{ source('raw_lightdash_system', 'saved_queries') }}
