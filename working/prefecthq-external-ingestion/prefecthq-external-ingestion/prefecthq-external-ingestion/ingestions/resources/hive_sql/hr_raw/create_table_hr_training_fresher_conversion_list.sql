CREATE TABLE IF NOT EXISTS hr_raw.hr_training_fresher_conversion_list (
  sequence_number VARCHAR,
  full_name VARCHAR,
  employee_code VARCHAR,
  onboard_date VARCHAR,
  conversion_date VARCHAR,
  role_base VARCHAR,
  level_region VARCHAR,
  department_n_2 VARCHAR,
  current_work_location VARCHAR,
  interview_result VARCHAR,
  interview_result_file VARCHAR,
  onboard_date_ts BIGINT,
  conversion_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_fresher_conversion_list',
   format = 'PARQUET'
);