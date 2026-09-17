CREATE TABLE IF NOT EXISTS jira_raw.task_operation (
          key STRING,
  summary STRING,
  issuetype STRING,
  status STRING,
  updated STRING,
  created STRING,
  assignee STRING,
  priority STRING,
  assignor STRING,
  work_group STRING,
  department_center STRING,
  start_date STRING,
  duedate STRING,
  source_file STRING,
  updated_ts BIGINT,
  created_ts BIGINT,
  start_date_ts BIGINT,
  duedate_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/task_operation'