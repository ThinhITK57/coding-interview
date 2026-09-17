CREATE TABLE IF NOT EXISTS jira_raw.project_versions (
          versionid BIGINT,
  projectkey STRING,
  projectname STRING,
  versionname STRING,
  description STRING,
  startdate STRING,
  releasedate STRING,
  released BOOLEAN,
  archived BOOLEAN,
  source_file STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/project_versions'