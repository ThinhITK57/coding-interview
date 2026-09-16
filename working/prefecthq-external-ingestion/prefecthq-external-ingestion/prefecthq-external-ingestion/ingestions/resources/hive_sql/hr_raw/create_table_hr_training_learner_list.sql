CREATE TABLE IF NOT EXISTS hr_raw.hr_training_learner_list (
  employee_code VARCHAR,
  full_name VARCHAR,
  employee_object VARCHAR,
  division_n VARCHAR,
  department_n_2 VARCHAR,
  class_code VARCHAR,
  class_name VARCHAR,
  course_name VARCHAR,
  learning_result VARCHAR,
  learning_duration_hours VARCHAR,
  start_date VARCHAR,
  end_date VARCHAR,
  month VARCHAR,
  year VARCHAR,
  lxp_status_note VARCHAR,
  start_date_ts BIGINT,
  end_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_learner_list',
   format = 'PARQUET'
);