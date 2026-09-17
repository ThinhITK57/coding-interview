CREATE TABLE IF NOT EXISTS noc_metrics_raw.disk_used_percent__mean (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRUCT<path:STRING>,
  time STRING,
  mean DOUBLE,
  time_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/disk_used_percent__mean'