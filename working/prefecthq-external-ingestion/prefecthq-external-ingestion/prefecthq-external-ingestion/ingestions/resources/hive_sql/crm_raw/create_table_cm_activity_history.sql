CREATE TABLE IF NOT EXISTS crm_raw.cm_activity_history (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_related_contact:STRING, cf_related_company:BIGINT, cf_related_opportunity:BIGINT>,
  created_at STRING,
  creator_id BIGINT,
  updated_at STRING,
  updater_id STRING,
  avatar STRING,
  recent_note STRING,
  links STRUCT<document_associations:STRING, notes:STRING>,
  record_type_id STRING,
  crawled_at_ts BIGINT,
  created_at_ts BIGINT,
  updated_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/cm_activity_history'