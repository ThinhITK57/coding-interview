CREATE TABLE IF NOT EXISTS crm_raw.cm_business_rules (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_type:STRING, cf_price_type:STRING, cf_parent_product:BIGINT, cf_child_product:STRING, cf_ratio_operator:STRING, cf_ratio_value:BIGINT, cf_ratio_unit:STRING, cf_active:BOOLEAN>,
  created_at STRING,
  creator_id BIGINT,
  updated_at STRING,
  updater_id BIGINT,
  avatar STRING,
  recent_note STRING,
  links STRUCT<document_associations:STRING, notes:STRING>,
  record_type_id STRING,
  crawled_at_ts BIGINT,
  created_at_ts BIGINT,
  updated_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/cm_business_rules'