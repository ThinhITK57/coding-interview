CREATE TABLE IF NOT EXISTS finance_raw.product_service_costs (
          date_key STRING,
  product_service_name STRING,
  product_service_code STRING,
  expense_code STRING,
  product_service_expense_category STRING,
  data_type STRING,
  market STRING,
  amount_million_vnd STRING,
  source_file STRING,
  date_key_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/product_service_costs'