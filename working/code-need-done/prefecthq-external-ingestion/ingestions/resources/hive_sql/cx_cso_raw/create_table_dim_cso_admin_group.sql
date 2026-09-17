CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_admin_group (
          id BIGINT,
  name STRING,
  description STRING,
  escalate_to STRING,
  unassigned_for STRING,
  agent_ids ARRAY<BIGINT>,
  created_at STRING,
  updated_at STRING,
  allow_agents_to_change_availability BOOLEAN,
  agent_availability_status BOOLEAN,
  business_calendar_id BIGINT,
  type STRING,
  automatic_agent_assignment STRUCT<enabled:BOOLEAN, type:STRING, settings:ARRAY<STRUCT<channel:STRING, assignment_type:STRING>>>,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_admin_group'