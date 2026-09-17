CREATE TABLE IF NOT EXISTS cx_survicate_raw.raw_dim_survicate_surveys (
          id STRING,
  type STRING,
  name STRING,
  created_at STRING,
  enabled BOOLEAN,
  responses BIGINT,
  launch STRUCT<start_at:STRING, end_at:STRING, responses_limit:STRING>,
  created_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-survicate-raw/dim_survicate_surveys'