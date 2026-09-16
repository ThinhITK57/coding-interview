CREATE TABLE IF NOT EXISTS crm_raw.raw_cm_catalog (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_category:STRING, cf_version:STRING, cf_max_discount:BIGINT, cf_item_type:STRING, cf_active:BOOLEAN, cf_market:BIGINT>,
  created_at STRING,
  creator_id BIGINT,
  updated_at STRING,
  updater_id BIGINT,
  avatar STRING,
  recent_note STRING,
  links STRUCT<document_associations:STRING, notes:STRING>,
  record_type_id STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/cm_catalog'