CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_agents (
          available BOOLEAN,
  occasional BOOLEAN,
  id BIGINT,
  ticket_scope BIGINT,
  created_at STRING,
  updated_at STRING,
  last_active_at STRING,
  available_since STRING,
  type STRING,
  contact STRUCT<active:BOOLEAN, email:STRING, job_title:STRING, language:STRING, last_login_at:STRING, mobile:STRING, name:STRING, phone:STRING, time_zone:STRING, created_at:STRING, updated_at:STRING, avatar:STRUCT<id:BIGINT, name:STRING, content_type:STRING, size:BIGINT, created_at:STRING, updated_at:STRING, attachment_url:STRING, thumb_url:STRING>>,
  deactivated BOOLEAN,
  signature STRING,
  agent_level_id BIGINT,
  focus_mode BOOLEAN,
  agent_operational_status STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  last_active_at_ts BIGINT,
  available_since_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_agents'