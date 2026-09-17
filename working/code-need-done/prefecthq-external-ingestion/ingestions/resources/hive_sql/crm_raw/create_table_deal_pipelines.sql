CREATE TABLE IF NOT EXISTS crm_raw.raw_deal_pipelines (
          partial BOOLEAN,
  id BIGINT,
  name STRING,
  position BIGINT,
  is_default BOOLEAN,
  rotting_days BIGINT,
  configs ARRAY<STRUCT<field_name:STRING, position:BIGINT, highlight:BOOLEAN>>,
  aggregated_field STRING,
  deal_stages ARRAY<STRUCT<id:BIGINT, value:STRING, name:STRING, position:BIGINT, forecast_type:STRING, deal_pipeline_id:BIGINT, choice_type:BIGINT, is_deleted:BOOLEAN, probability:BIGINT, updated_at:STRING>>,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/deal_pipelines'