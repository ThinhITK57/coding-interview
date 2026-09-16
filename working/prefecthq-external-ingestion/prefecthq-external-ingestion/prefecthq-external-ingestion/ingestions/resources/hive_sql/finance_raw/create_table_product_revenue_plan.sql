CREATE TABLE IF NOT EXISTS finance_raw.product_revenue_plan (
          row_no BIGINT,
  product_code STRING,
  product_name STRING,
  product_group STRING,
  target_value BIGINT,
  period STRING,
  year BIGINT,
  month BIGINT,
  target_type STRING,
  currency_code STRING,
  snapshot_at STRING,
  snapshot_version BIGINT,
  crawled_at_ts BIGINT,
  snapshot_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/product_revenue_plan'