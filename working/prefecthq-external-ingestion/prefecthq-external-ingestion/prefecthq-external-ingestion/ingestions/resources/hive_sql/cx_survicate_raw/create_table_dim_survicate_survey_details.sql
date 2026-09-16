CREATE TABLE IF NOT EXISTS cx_survicate_raw.raw_dim_survicate_survey_details (
          id STRING,
  type STRING,
  name STRING,
  created_at STRING,
  enabled BOOLEAN,
  responses BIGINT,
  launch STRUCT<start_at:STRING, end_at:STRING, responses_limit:STRING>,
  author STRUCT<name:STRING, email:STRING>,
  folder STRING,
  first_response_at STRING,
  last_response_at STRING,
  attributes ARRAY<STRING>,
  survey_id STRING,
  created_at_ts BIGINT,
  first_response_at_ts BIGINT,
  last_response_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-survicate-raw/dim_survicate_survey_details'