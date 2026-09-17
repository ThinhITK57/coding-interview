CREATE TABLE IF NOT EXISTS cx_cso_raw.cx_company (
          id STRING,
  name STRING,
  company_alias STRING,
  tax_code STRING,
  customer_segment_l1 STRING,
  customer_segment_l2 STRING,
  customer_segment_l3 STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/cx_company'