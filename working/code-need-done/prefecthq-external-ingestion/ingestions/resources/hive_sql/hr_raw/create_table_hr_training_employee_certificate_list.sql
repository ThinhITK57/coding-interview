CREATE TABLE IF NOT EXISTS hr_raw.hr_training_employee_certificate_list (
  sequence_number VARCHAR,
  employee_code VARCHAR,
  full_name VARCHAR,
  division_n VARCHAR,
  department_n_2 VARCHAR,
  role_base VARCHAR,
  level VARCHAR,
  employee_object VARCHAR,
  certificate_check_flag VARCHAR,
  certificate_status VARCHAR,
  certificate_name VARCHAR,
  certificate_level VARCHAR,
  certificate_result VARCHAR,
  certificate_type VARCHAR,
  certificate_issue_date VARCHAR,
  month VARCHAR,
  year VARCHAR,
  certificate_validity_period VARCHAR,
  certificate_expiry_date VARCHAR,
  certificate_current_status VARCHAR,
  certificate_issue_date_ts BIGINT,
  certificate_expiry_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_employee_certificate_list',
   format = 'PARQUET'
);