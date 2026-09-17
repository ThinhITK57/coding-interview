CREATE TABLE IF NOT EXISTS cx_cso_raw.cx_dim_journey (
          id STRING,
  journey_name STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/cx_dim_journey'