CREATE TABLE IF NOT EXISTS finance_raw.dim_product_category_code (
          category_code STRING,
  product_category STRING,
  product_erp_name STRING,
  item_type STRING,
  source_file STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/dim_product_category_code'