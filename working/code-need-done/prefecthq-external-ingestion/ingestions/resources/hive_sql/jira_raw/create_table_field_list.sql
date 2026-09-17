CREATE TABLE IF NOT EXISTS jira_raw.field_list (
          id STRING,
  name STRING,
  custom BOOLEAN,
  orderable BOOLEAN,
  navigable BOOLEAN,
  searchable BOOLEAN,
  clauseNames ARRAY<STRING>,
  schema STRUCT<type:STRING, custom:STRING, customId:BIGINT, system:STRING, items:STRING>,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/field_list'