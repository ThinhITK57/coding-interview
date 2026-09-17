CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_company_fields (
          id BIGINT,
  name STRING,
  label STRING,
  position BIGINT,
  required_for_agents BOOLEAN,
  type STRING,
  default BOOLEAN,
  created_at STRING,
  updated_at STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  choices ARRAY<STRING>,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_company_fields'