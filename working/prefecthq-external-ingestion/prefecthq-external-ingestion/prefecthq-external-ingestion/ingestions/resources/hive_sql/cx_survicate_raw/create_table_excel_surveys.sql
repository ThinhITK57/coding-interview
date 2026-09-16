CREATE TABLE IF NOT EXISTS cx_survicate_raw.excel_surveys (
          survey_id STRING,
  survey_name STRING,
  created_at STRING,
  folder STRING,
  question_count STRING,
  response_count STRING,
  survey_active STRING,
  journey_id STRING,
  journey STRING,
  touchpoint STRING,
  product_service STRING,
  note STRING,
  allow_negative_feedback_ticket STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  created_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/survicate/data/excel_surveys'