CREATE TABLE IF NOT EXISTS finance_raw.costs (
          date STRING,
  data_type STRING,
  expense_category_level_2 STRING,
  expense_code STRING,
  amount_million_vnd STRING,
  source_file STRING,
  date_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/costs'