CREATE TABLE IF NOT EXISTS hr_raw.hr_training_trainee_list (
  sequence_number VARCHAR,
  full_name VARCHAR,
  employee_code VARCHAR,
  training_program VARCHAR,
  training_major VARCHAR,
  training_phase VARCHAR,
  ojt_start_date VARCHAR,
  ojt_end_date VARCHAR,
  ojt_progress VARCHAR,
  average_score VARCHAR,
  classification VARCHAR,
  current_status VARCHAR,
  training_result_link VARCHAR,
  employment_type VARCHAR,
  work_location VARCHAR,
  business_email VARCHAR,
  mentor_email VARCHAR,
  filename VARCHAR,
  crawled_at_ts BIGINT,
  ojt_start_date_ts BIGINT,
  ojt_end_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_trainee_list',
   format = 'PARQUET'
);