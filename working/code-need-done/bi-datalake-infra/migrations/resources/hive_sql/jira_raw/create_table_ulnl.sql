CREATE TABLE IF NOT EXISTS jira_raw.ulnl (
          key STRING,
  projectkey STRING,
  projectname STRING,
  summary STRING,
  issuetype STRING,
  status STRING,
  assignee STRING,
  resolved STRING,
  ttsx_product STRING,
  total_net_effort DOUBLE,
  updated STRING,
  source_file STRING,
  resolved_ts BIGINT,
  updated_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/ulnl'