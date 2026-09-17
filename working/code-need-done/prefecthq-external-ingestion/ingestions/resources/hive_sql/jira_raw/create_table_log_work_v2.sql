CREATE TABLE IF NOT EXISTS jira_raw.log_work_v2 (
          issueid BIGINT,
  worklogid BIGINT,
  projectkey STRING,
  comment STRING,
  timespent BIGINT,
  updated BIGINT,
  started BIGINT,
  created BIGINT,
  authorfullname STRING,
  authorusername STRING,
  categoryname STRING,
  source_file STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/log_work_v2'