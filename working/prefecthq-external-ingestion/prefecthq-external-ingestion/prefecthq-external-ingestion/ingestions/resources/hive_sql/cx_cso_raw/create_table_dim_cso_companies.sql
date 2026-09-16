CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_companies (
          id BIGINT,
  name STRING,
  description STRING,
  note STRING,
  domains ARRAY<STRING>,
  created_at STRING,
  updated_at STRING,
  custom_fields STRUCT<alias:STRING, tax_code:STRING, company_segment:STRING, company_type:STRING>,
  health_score STRING,
  account_tier STRING,
  renewal_date STRING,
  industry STRING,
  org_company_id STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_companies'