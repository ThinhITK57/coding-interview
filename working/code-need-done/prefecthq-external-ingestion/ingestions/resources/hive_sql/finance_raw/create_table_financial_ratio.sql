CREATE TABLE IF NOT EXISTS finance_raw.financial_ratio (
          report_date STRING,
  description STRING,
  value STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  report_date_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/financial_ratio'