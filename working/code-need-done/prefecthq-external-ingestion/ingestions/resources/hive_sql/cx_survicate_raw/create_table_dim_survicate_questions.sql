CREATE TABLE IF NOT EXISTS cx_survicate_raw.raw_dim_survicate_questions (
          id BIGINT,
  type STRING,
  question STRING,
  introduction STRING,
  survey_id STRING,
  answer_choices ARRAY<STRUCT<id:BIGINT, content:STRING>>,
  columns ARRAY<STRING>,
  fields ARRAY<STRUCT<type:STRING, label:STRING>>,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-survicate-raw/dim_survicate_questions'