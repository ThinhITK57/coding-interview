CREATE TABLE IF NOT EXISTS jira_raw.process_compliance (
          key STRING,
  summary STRING,
  issuetype STRING,
  duedate STRING,
  ttqt_product_name STRING,
  process_compliance_rate DOUBLE,
  updated STRING,
  source_file STRING,
  updated_ts BIGINT,
  duedate_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/process_compliance'