CREATE TABLE IF NOT EXISTS crm_raw.raw_deal_payment_statuses (
          id BIGINT,
  name STRING,
  position BIGINT,
  partial BOOLEAN,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/deal_payment_statuses'