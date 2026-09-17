CREATE TABLE IF NOT EXISTS finance_raw.cost_plan (
          date_key STRING,
  expense_code STRING,
  data_type STRING,
  expense_category_level_2 STRING,
  amount_million_vnd STRING,
  source_file STRING,
  date_key_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/cost_plan'