CREATE TABLE IF NOT EXISTS crm_raw.raw_business_types (
          id BIGINT,
  name STRING,
  position BIGINT,
  partial BOOLEAN,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/business_types'