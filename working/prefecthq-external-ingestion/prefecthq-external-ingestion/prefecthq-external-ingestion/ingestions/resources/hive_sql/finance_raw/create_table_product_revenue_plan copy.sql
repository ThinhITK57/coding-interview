CREATE TABLE IF NOT EXISTS finance_raw.product_revenue_plan (
          product_group STRING,
  product_code STRING,
  product_name STRING,
  year BIGINT,
  month BIGINT,
  period STRING,
  currency_code STRING,
  snapshot_at STRING,
  snapshot_version BIGINT,
  must BIGINT,
  nice BIGINT,
  crawled_at_ts BIGINT,
  snapshot_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/product_revenue_plan'