CREATE TABLE IF NOT EXISTS finance_raw.finance_plan (
          plan_date STRING,
  company STRING,
  plan_viettel_group STRING,
  plan_must STRING,
  plan_nice STRING,
  segment STRING,
  subgroup STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  plan_date_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/finance_plan'