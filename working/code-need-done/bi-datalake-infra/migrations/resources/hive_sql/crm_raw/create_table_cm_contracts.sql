CREATE TABLE IF NOT EXISTS crm_raw.cm_contracts (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_contract_id:STRING, cf_type:STRING, cf_signing_method:STRING, cf_company:BIGINT, cf_alias:STRING, cf_tax_code:STRING, cf_group:STRING, cf_segment:STRING, cf_segment2:STRING, cf_segment3:STRING, cf_products_in_contract:STRING, cf_deployed_products:STRING, cf_status:STRING, cf_sign_date:STRING, cf_fac_date:STRING, cf_duration:DOUBLE, cf_expire_date:STRING, cf_usd_to_vnd:DOUBLE, cf_revenue:DOUBLE, cf_currency:STRING, cf_vat:BIGINT, cf_vcs_revenue:BIGINT, cf_viettel_revenue:BIGINT, cf_partner:STRING, cf_opportunity:BIGINT, cf_bidding_required:BOOLEAN, cf__actual:BOOLEAN, cf__opp_id:STRING>,
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
        LOCATION 's3a://vcs-raw/crm-raw/cm_contracts'