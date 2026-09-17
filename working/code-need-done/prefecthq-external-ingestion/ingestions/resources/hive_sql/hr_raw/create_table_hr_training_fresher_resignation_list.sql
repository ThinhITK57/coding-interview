CREATE TABLE IF NOT EXISTS hr_raw.hr_training_fresher_resignation_list (
  sequence_number VARCHAR,
  full_name VARCHAR,
  employee_code VARCHAR,
  onboard_date VARCHAR,
  resigned_date VARCHAR,
  resignation_reason VARCHAR,
  training_phase VARCHAR,
  training_major VARCHAR,
  mentor_name VARCHAR,
  department_n_2 VARCHAR,
  ojt_start_date VARCHAR,
  ojt_end_date VARCHAR,
  current_work_location VARCHAR,
  onboard_date_ts BIGINT,
  resigned_date_ts BIGINT,
  ojt_start_date_ts BIGINT,
  ojt_end_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_fresher_resignation_list',
   format = 'PARQUET'
)
