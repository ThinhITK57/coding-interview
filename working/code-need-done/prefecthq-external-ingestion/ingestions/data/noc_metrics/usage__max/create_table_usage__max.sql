CREATE TABLE IF NOT EXISTS noc_metrics_raw.usage__max (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRING,
  time STRING,
  max DOUBLE,
  time_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/usage__max'