CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_roles (
          id BIGINT,
  name STRING,
  description STRING,
  default BOOLEAN,
  created_at STRING,
  updated_at STRING,
  agent_type BIGINT,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_roles'