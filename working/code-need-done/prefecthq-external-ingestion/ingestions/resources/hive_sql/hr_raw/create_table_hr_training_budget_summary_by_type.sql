CREATE TABLE IF NOT EXISTS hr_raw.hr_training_budget_summary_by_type (
  sequence_number VARCHAR,
  budget_type VARCHAR,
  budget_amount VARCHAR,
  approval_request_amount VARCHAR,
  spent_amount VARCHAR,
  remaining_amount VARCHAR,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_budget_summary_by_type',
   format = 'PARQUET'
);