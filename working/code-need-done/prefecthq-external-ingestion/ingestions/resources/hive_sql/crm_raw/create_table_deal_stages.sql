CREATE TABLE IF NOT EXISTS crm_raw.raw_deal_stages (
          partial BOOLEAN,
  id BIGINT,
  name STRING,
  position BIGINT,
  forecast_type STRING,
  updated_at STRING,
  deal_pipeline_id BIGINT,
  choice_type BIGINT,
  probability BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/deal_stages'