CREATE TABLE IF NOT EXISTS finance_raw.actual_cost (
          report_date STRING,
  report_year STRING,
  report_month STRING,
  old_category STRING,
  category_code STRING,
  cost_group STRING,
  base_currency_amount STRING,
  currency_code STRING,
  territory_name STRING,
  product_category STRING,
  unit_level_1 STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  report_date_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/actual_cost'