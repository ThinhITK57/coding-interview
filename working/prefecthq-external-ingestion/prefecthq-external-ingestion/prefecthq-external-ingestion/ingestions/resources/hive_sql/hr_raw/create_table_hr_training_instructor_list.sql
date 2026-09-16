CREATE TABLE IF NOT EXISTS hr_raw.hr_training_instructor_list (
  employee_code VARCHAR,
  full_name VARCHAR,
  business_email VARCHAR,
  instructor_group VARCHAR,
  division_n VARCHAR,
  department_n_2 VARCHAR,
  class_code VARCHAR,
  class_name VARCHAR,
  teaching_duration VARCHAR,
  instructor_fee VARCHAR,
  instructor_rating VARCHAR,
  start_date VARCHAR,
  end_date VARCHAR,
  month VARCHAR,
  year VARCHAR,
  start_date_ts BIGINT,
  end_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_instructor_list',
   format = 'PARQUET'
);