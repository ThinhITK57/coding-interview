CREATE TABLE IF NOT EXISTS crm_raw.raw_currencies (
          partial BOOLEAN,
  id BIGINT,
  is_active BOOLEAN,
  currency_code STRING,
  exchange_rate STRING,
  currency_type BIGINT,
  schedule_info STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/currencies'