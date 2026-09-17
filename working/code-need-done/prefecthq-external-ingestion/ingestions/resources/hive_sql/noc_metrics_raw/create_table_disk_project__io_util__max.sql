CREATE TABLE IF NOT EXISTS noc_metrics_raw.disk_project__io_util__max (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRUCT<name:STRING>,
  time STRING,
  max DOUBLE,
  time_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/disk_project__io_util__max'