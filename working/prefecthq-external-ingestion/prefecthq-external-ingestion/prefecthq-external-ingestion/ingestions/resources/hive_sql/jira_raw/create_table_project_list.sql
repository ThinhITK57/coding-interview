CREATE TABLE IF NOT EXISTS jira_raw.project_list (
          expand STRING,
  self STRING,
  id STRING,
  key STRING,
  description STRING,
  lead STRUCT<self:STRING, key:STRING, name:STRING, avatarUrls:STRUCT<48x48:STRING, 24x24:STRING, 16x16:STRING, 32x32:STRING>, displayName:STRING, active:BOOLEAN>,
  name STRING,
  avatarUrls STRUCT<48x48:STRING, 24x24:STRING, 16x16:STRING, 32x32:STRING>,
  projectKeys ARRAY<STRING>,
  projectTypeKey STRING,
  archived BOOLEAN,
  projectCategory STRUCT<self:STRING, id:STRING, name:STRING, description:STRING>,
  url STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/project_list'