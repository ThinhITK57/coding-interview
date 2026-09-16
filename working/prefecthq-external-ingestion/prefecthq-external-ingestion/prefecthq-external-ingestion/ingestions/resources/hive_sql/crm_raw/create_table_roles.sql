CREATE TABLE IF NOT EXISTS crm_raw.roles (
          id BIGINT,
  name STRING,
  created_by STRING,
  updated_by STRING,
  updated_at STRING,
  created_at STRING,
  default_role BOOLEAN,
  addons ARRAY<STRING>,
  internal_name STRING,
  licensed_users_count BIGINT,
  user_ids ARRAY<BIGINT>,
  updated_at_ts BIGINT,
  created_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/roles'