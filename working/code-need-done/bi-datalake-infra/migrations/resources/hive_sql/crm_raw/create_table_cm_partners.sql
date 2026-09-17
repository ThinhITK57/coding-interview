CREATE TABLE IF NOT EXISTS crm_raw.raw_cm_partners (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_document_number:STRING, cf_alias:STRING, cf_address:STRING, cf_country:STRING, cf_contact_name:STRING, cf_contact_position:STRING, cf_contact_mobile:STRING, cf_contact_email:STRING, cf_effective_date:STRING, cf_expiration_date:STRING, cf_type:STRING, cf_document_format:STRING, cf_status:STRING>,
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
        LOCATION 's3a://vcs-raw/crm-raw/cm_partners'