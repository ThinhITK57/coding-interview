CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_time_entries (
          billable BOOLEAN,
  note STRING,
  id BIGINT,
  timer_running BOOLEAN,
  agent_id BIGINT,
  ticket_id BIGINT,
  company_id BIGINT,
  time_spent STRING,
  executed_at STRING,
  start_time STRING,
  created_at STRING,
  updated_at STRING,
  time_spent_in_seconds BIGINT,
  executed_at_ts BIGINT,
  start_time_ts BIGINT,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_time_entries'