CREATE TABLE IF NOT EXISTS crm_raw.internal_am_dictionary (
          stt STRING,
  company_alias STRING,
  am_username STRING,
  internal_am_group STRING,
  am_user_id STRING,
  am_full_name STRING,
  org_name STRING,
  note STRING,
  source_file STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/internal_am_dictionary';


      
CREATE TABLE hive.crm_raw.internal_am_dictionary (
  
  stt varchar,
  company_alias varchar,
  am_username varchar,
  internal_am_group varchar,
  am_user_id varchar,
  am_full_name varchar,
  org_name varchar,
  note varchar,
  source_file varchar,
  crawled_at_ts BIGINT
)
WITH (
   external_location = '/opt/datasets/crawlers/vcs_raw/crm-raw/data/internal_am_dictionary',
   format = 'PARQUET'
);