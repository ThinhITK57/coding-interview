CREATE TABLE IF NOT EXISTS jira_raw.sprints (
          id BIGINT,
  name STRING,
  rapid_view_id BIGINT,
  sequence BIGINT,
  goal STRING,
  closed BOOLEAN,
  started BOOLEAN,
  auto_start_stop BOOLEAN,
  start_date BIGINT,
  end_date BIGINT,
  complete_date BIGINT,
  activated_date BIGINT,
  source_file STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/jira/data/sprints'