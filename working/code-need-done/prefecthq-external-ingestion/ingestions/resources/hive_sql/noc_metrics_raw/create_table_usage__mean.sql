CREATE TABLE IF NOT EXISTS noc_metrics_raw.usage__mean (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRING,
  time STRING,
  mean DOUBLE,
  time_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/usage__mean'