CREATE TABLE IF NOT EXISTS noc_metrics_raw.cpu_usage_average__mean (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRUCT<name:STRING>,
  time STRING,
  mean DOUBLE,
  time_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/cpu_usage_average__mean'