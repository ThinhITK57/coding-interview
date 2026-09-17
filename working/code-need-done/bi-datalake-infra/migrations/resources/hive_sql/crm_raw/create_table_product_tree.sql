CREATE TABLE IF NOT EXISTS crm_raw.product_tree (
          category STRING,
  version STRING,
  product_type STRING,
  sub_type STRING,
  license STRING,
  price_type STRING,
  deployment_type STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/product_tree'