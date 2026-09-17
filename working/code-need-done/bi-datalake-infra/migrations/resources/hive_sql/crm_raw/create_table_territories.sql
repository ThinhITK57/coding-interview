CREATE TABLE IF NOT EXISTS crm_raw.raw_territories (
          id BIGINT,
  name STRING,
  position BIGINT,
  parent_territory_id STRING,
  tree_id STRING,
  level_id STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/territories'