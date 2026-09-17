CREATE TABLE IF NOT EXISTS cx_survicate_raw.raw_fact_survicate_personal_data (
          responses BIGINT,
  respondents BIGINT,
  insights_hub STRUCT<authors:BIGINT, author_attributes:BIGINT, notes:BIGINT, notes_attributes:BIGINT, total:BIGINT>,
  email STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-survicate-raw/fact_survicate_personal_data'