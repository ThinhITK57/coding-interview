CREATE TABLE IF NOT EXISTS noc_metrics_raw.free_space__capacity (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRUCT<ds_name:STRING>,
  time STRING,
  last_last DOUBLE,
  time_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/free_space__capacity'