CREATE TABLE IF NOT EXISTS finance_raw.cost_items (
          date STRING,
  data_type STRING,
  expense_category_level_2 STRING,
  expense_code STRING,
  amount_million_vnd STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  date_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/cost_items'