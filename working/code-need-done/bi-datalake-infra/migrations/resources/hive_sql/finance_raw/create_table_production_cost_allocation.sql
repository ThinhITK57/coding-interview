CREATE TABLE IF NOT EXISTS finance_raw.production_cost_allocation (
          date_key STRING,
  product_service_name STRING,
  product_service_code STRING,
  expense_code STRING,
  product_service_expense_category STRING,
  data_type STRING,
  market STRING,
  amount_million_vnd STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  date_key_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/production_cost_allocation'