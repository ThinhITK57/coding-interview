CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_groups (
          id BIGINT,
  name STRING,
  description STRING,
  escalate_to STRING,
  unassigned_for STRING,
  business_hour_id BIGINT,
  group_type STRING,
  created_at STRING,
  updated_at STRING,
  auto_ticket_assign BIGINT,
  agent_availability_status BOOLEAN,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_groups'