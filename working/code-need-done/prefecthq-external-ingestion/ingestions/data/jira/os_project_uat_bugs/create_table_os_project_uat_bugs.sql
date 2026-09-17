CREATE TABLE IF NOT EXISTS jira_raw.os_project_uat_bugs (
          key STRING,
  summary STRING,
  issuetype STRING,
  status STRING,
  updated STRING,
  created STRING,
  projectkey STRING,
  projectname STRING,
  source_file STRING,
  updated_ts BIGINT,
  created_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/os_project_uat_bugs'