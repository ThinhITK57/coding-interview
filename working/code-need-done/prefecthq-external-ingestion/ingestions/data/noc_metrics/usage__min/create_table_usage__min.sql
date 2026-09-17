CREATE TABLE IF NOT EXISTS noc_metrics_raw.usage__min (
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
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/usage__min'