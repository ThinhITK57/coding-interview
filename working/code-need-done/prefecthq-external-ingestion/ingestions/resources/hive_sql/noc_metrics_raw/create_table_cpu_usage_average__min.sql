CREATE TABLE IF NOT EXISTS noc_metrics_raw.cpu_usage_average__min (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRUCT<name:STRING>,
  time STRING,
  min DOUBLE,
  time_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/cpu_usage_average__min'