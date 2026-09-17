CREATE TABLE IF NOT EXISTS finance_raw.contract_collected_invoices (
          stt STRING,
  company STRING,
  transaction_code STRING,
  invoice_number STRING,
  contract_order_number STRING,
  pmtc_code STRING,
  description STRING,
  customer_name STRING,
  original_currency STRING,
  initial_receivable STRING,
  collected STRING,
  outstanding_receivable STRING,
  notes STRING,
  invoice_date STRING,
  payment_deadline STRING,
  payment_date STRING,
  overdue_months STRING,
  account_manager STRING,
  source_file STRING,
  crawled_at_ts BIGINT,
  invoice_date_ts BIGINT,
  payment_deadline_ts BIGINT,
  payment_date_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/finance-raw/contract_collected_invoices'