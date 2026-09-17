CREATE TABLE IF NOT EXISTS noc_metrics_raw.la_per_cpu__min (
          name STRING,
  customer STRING,
  host STRING,
  project STRING,
  tags STRING,
  time STRING,
  min DOUBLE,
  time_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/noc_metrics/data/la_per_cpu__min'