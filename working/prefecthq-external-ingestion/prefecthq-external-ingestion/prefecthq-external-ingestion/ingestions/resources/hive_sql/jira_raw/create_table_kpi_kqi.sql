CREATE TABLE IF NOT EXISTS jira_raw.kpi_kqi (
          key STRING,
  summary STRING,
  quarterly_kpi_result DOUBLE,
  monthly_accumulated_result DOUBLE,
  execution_result DOUBLE,
  reporting_date STRING,
  unit_of_measure DOUBLE,
  target STRING,
  service_product STRING,
  condition STRING,
  updated STRING,
  source_file STRING,
  reporting_date_ts BIGINT,
  updated_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/kpi_kqi'