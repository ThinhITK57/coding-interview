CREATE TABLE IF NOT EXISTS hr_raw.hr_training_online_learning_list (
  sequence_number VARCHAR,
  employee_code VARCHAR,
  full_name VARCHAR,
  business_email VARCHAR,
  department_n_2 VARCHAR,
  course_count VARCHAR,
  learning_platform VARCHAR,
  course_name VARCHAR,
  filename VARCHAR,
  crawled_at_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_online_learning_list',
   format = 'PARQUET'
);