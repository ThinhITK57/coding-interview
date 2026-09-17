CREATE TABLE IF NOT EXISTS jira_raw.log_work (
          projectkey STRING,
  issueid BIGINT,
  issuekey STRING,
  summary STRING,
  worklogid BIGINT,
  author STRING,
  started STRING,
  timespent_hours DOUBLE,
  comment STRING,
  created STRING,
  updated STRING,
  source_file STRING
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/log_work'