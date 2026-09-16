CREATE TABLE IF NOT EXISTS noc_metrics_raw.net_customer__util__min (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRUCT<speed:STRING>,
  time STRING,
  min DOUBLE,
  time_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/net_customer__util__min'