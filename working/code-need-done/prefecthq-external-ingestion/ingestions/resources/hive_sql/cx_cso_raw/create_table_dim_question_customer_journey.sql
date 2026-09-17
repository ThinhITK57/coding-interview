CREATE TABLE IF NOT EXISTS cx_cso_raw.dim_question_customer_journey (
          survey_id STRING,
  survey_name STRING,
  question_id STRING,
  question_type STRING,
  question_text STRING,
  answer_choice_id STRING,
  answer_choice_content STRING,
  question_type__h STRING,
  active STRING,
  kpi STRING,
  product_category STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_question_customer_journey'