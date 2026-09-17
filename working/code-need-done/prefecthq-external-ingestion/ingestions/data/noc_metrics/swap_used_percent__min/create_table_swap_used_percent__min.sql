CREATE TABLE IF NOT EXISTS noc_metrics_raw.swap_used_percent__min (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRING,
  time STRING,
  min DOUBLE,
  time_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/swap_used_percent__min'