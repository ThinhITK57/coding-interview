CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_email_configs (
          id BIGINT,
  name STRING,
  product_id BIGINT,
  to_email STRING,
  reply_email STRING,
  group_id STRING,
  primary_role BOOLEAN,
  active BOOLEAN,
  created_at STRING,
  updated_at STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_email_configs'