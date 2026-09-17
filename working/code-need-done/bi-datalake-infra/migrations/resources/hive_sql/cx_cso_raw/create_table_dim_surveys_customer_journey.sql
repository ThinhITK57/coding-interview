CREATE TABLE IF NOT EXISTS cx_cso_raw.dim_surveys_customer_journey (
          survey_id STRING,
  survey_name STRING,
  journey STRING,
  customer_touchpoint STRING,
  product_category STRING,
  note STRING,
  created_ticket_vs_negative STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_surveys_customer_journey'