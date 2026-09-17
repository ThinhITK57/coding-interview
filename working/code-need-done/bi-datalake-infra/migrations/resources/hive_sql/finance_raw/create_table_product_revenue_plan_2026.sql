CREATE TABLE IF NOT EXISTS finance_raw.product_revenue_plan_2026 (
          customer_group_name STRING,
  customer_subgroup_name STRING,
  group_name STRING,
  subgroup_name STRING,
  service_code STRING,
  service_name STRING,
  priority_must STRING,
  priority_should STRING,
  priority_nice STRING,
  amount_unit STRING,
  report_month STRING,
  report_year STRING,
  report_date STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  report_date_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/product_revenue_plan_2026'