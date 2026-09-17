CREATE TABLE IF NOT EXISTS cx_survicate_raw.excel_survey_questions (
          survey_id STRING,
  survey_name STRING,
  question_id STRING,
  question_type STRING,
  question_text STRING,
  answer_choice_id STRING,
  answer_choice_content STRING,
  active STRING,
  kpi STRING,
  product_service STRING,
  layer STRING,
  kpi_checked STRING,
  source_file STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION '/opt/datasets/crawlers/vcs/survicate/data/excel_survey_questions'