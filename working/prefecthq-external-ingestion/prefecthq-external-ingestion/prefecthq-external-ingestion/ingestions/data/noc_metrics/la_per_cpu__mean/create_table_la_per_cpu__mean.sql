CREATE TABLE IF NOT EXISTS noc_metrics_raw.la_per_cpu__mean (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRING,
  time STRING,
  mean DOUBLE,
  time_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/la_per_cpu__mean'