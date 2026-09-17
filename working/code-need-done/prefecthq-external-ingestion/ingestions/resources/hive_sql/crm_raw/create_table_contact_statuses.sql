CREATE TABLE IF NOT EXISTS crm_raw.raw_contact_statuses (
          id BIGINT,
  name STRING,
  position BIGINT,
  partial BOOLEAN,
  forecast_type STRING,
  lifecycle_stage_id BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/contact_statuses'