CREATE TABLE IF NOT EXISTS cx_cso_raw.cx_product_category (
          issue_category STRING,
  product_category_code STRING,
  product_category STRING,
  nan STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/cx_product_category'