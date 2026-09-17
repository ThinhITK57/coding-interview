CREATE TABLE IF NOT EXISTS jira_raw.defect_task (
          key STRING,
  summary STRING,
  status STRING,
  resolved STRING,
  priority STRING,
  updated STRING,
  created STRING,
  projectkey STRING,
  projectname STRING,
  source_file STRING,
  updated_ts BIGINT,
  created_ts BIGINT,
  resolved_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/defect_task'