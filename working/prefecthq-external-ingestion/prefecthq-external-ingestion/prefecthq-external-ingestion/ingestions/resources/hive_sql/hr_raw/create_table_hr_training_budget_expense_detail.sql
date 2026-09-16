CREATE TABLE IF NOT EXISTS hr_raw.hr_training_budget_expense_detail (
  sequence_number VARCHAR,
  expense_description VARCHAR,
  expense_category VARCHAR,
  quarter VARCHAR,
  proposal_amount VARCHAR,
  accounted_amount VARCHAR,
  amount_difference VARCHAR,
  settlement_date VARCHAR,
  note VARCHAR,
  settlement_date_ts BIGINT,
  snapshot_date VARCHAR,
  snapshot_date_ts BIGINT,
  filename VARCHAR,
  crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/hr-raw/hr_training_budget_expense_detail',
   format = 'PARQUET'
)