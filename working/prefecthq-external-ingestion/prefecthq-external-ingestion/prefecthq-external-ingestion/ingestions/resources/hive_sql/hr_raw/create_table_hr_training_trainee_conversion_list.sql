CREATE TABLE IF NOT EXISTS hr_raw.hr_training_trainee_conversion_list (
  sequence_number VARCHAR,
  full_name VARCHAR,
  training_program VARCHAR,
  trainee_id VARCHAR,
  conversion_date VARCHAR,
  role_base VARCHAR,
  level_region VARCHAR,
  employee_code VARCHAR,
  training_phase VARCHAR,
  employment_type VARCHAR,
  expected_full_time_date VARCHAR,
  training_major VARCHAR,
  mentor_email VARCHAR,
  team VARCHAR,
  department_n_2 VARCHAR,
  interview_round_2_result VARCHAR,
  training_result_link VARCHAR,
  training_score VARCHAR,
  filename VARCHAR,
  crawled_at_ts BIGINT,
  conversion_date_ts BIGINT,
  expected_full_time_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_trainee_conversion_list',
   format = 'PARQUET'
);