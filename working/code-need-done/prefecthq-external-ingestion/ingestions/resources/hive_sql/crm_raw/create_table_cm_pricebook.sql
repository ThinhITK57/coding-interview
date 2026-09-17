CREATE TABLE IF NOT EXISTS crm_raw.raw_cm_pricebook (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_sku:STRING, cf_type:STRING, cf_sub_type:STRING, cf_license:STRING, cf_price_type:STRING, cf_package:STRING, cf_price:DOUBLE, cf_currency:STRING, cf_min:BIGINT, cf_max:BIGINT, cf_unit:STRING, cf_is_quantity_based:BOOLEAN, cf_is_related_csmp:BOOLEAN, cf_csmp_discount:BIGINT, cf_csmp_discount_silver:BIGINT, cf_csmp_discount_gold:BIGINT, cf_csmp_discount_diamond:BIGINT, cf_catalog:BIGINT, cf_related_pricebook:BIGINT>,
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
        LOCATION 's3a://vcs-raw/crm-raw/cm_pricebook'