CREATE TABLE IF NOT EXISTS crm_raw.raw_users (
          id BIGINT,
  display_name STRING,
  email STRING,
  is_active BOOLEAN,
  work_number STRING,
  mobile_number STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/users'