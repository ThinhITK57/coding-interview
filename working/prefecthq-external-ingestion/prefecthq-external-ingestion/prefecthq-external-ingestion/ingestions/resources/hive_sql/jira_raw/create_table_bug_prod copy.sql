CREATE TABLE IF NOT EXISTS jira_raw.bug_prod (
          key STRING,
  projectkey STRING,
  projectname STRING,
  summary STRING,
  issuetype STRING,
  status STRING,
  assignee STRING,
  resolved STRING,
  priority STRING,
  created STRING,
  updated STRING,
  duedate STRING,
  source_file STRING,
  created_ts BIGINT,
  updated_ts BIGINT,
  resolved_ts BIGINT,
  duedate_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/bug_prod'