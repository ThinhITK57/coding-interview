CREATE SCHEMA hive.finance_raw 
with (location='s3a://vcs-raw/finance-raw/');

-- hive.finance_raw.actual_cost definition

CREATE TABLE hive.finance_raw.actual_cost (
   report_date date,
   report_year integer,
   report_month integer,
   old_category varchar,
   category_code varchar,
   cost_group varchar,
   base_currency_amount decimal(18, 2),
   currency_code varchar,
   territory_name varchar,
   product_category varchar,
   unit_level_1 varchar,
   source_file varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.actual_revenue definition

CREATE TABLE hive.finance_raw.actual_revenue (
   share varchar,
   report_date varchar,
   revenue varchar,
   customer_name varchar,
   old_category varchar,
   payment_request_description varchar,
   segment_level_3 varchar,
   category_code varchar,
   revenue_group varchar,
   product_type varchar,
   is_soc varchar,
   product_service_group varchar,
   territory varchar,
   vat_amount varchar,
   account_manager varchar,
   month varchar,
   sales_channel varchar,
   department varchar,
   presale_owner varchar,
   payment_stage varchar,
   share_2 varchar,
   revenue_2 varchar,
   invoice_issue_date varchar,
   customer_segment varchar,
   revenue_recognition_type varchar,
   customer_group varchar,
   revenue_allocated_unit varchar,
   source_file varchar,
   crawled_at_ts bigint,
   report_date_ts bigint,
   invoice_issue_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.contract_collected_invoices definition

CREATE TABLE hive.finance_raw.contract_collected_invoices (
   stt varchar,
   company varchar,
   transaction_code varchar,
   invoice_number varchar,
   contract_order_number varchar,
   pmtc_code varchar,
   description varchar,
   customer_name varchar,
   original_currency varchar,
   initial_receivable varchar,
   collected varchar,
   outstanding_receivable varchar,
   notes varchar,
   invoice_date varchar,
   payment_deadline varchar,
   payment_date varchar,
   overdue_months varchar,
   account_manager varchar,
   source_file varchar,
   crawled_at_ts bigint,
   invoice_date_ts bigint,
   payment_deadline_ts bigint,
   payment_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.cost_items definition

CREATE TABLE hive.finance_raw.cost_items (
   report_date varchar,
   data_type varchar,
   expense_category_level_2 varchar,
   expense_code varchar,
   amount_million_vnd varchar,
   source_file varchar,
   crawled_at_ts bigint,
   report_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.cost_plan definition

CREATE TABLE hive.finance_raw.cost_plan (
   date_key varchar,
   expense_code varchar,
   data_type varchar,
   expense_category_level_2 varchar,
   amount_million_vnd varchar,
   source_file varchar,
   date_key_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.debt_report definition

CREATE TABLE hive.finance_raw.debt_report (
   report_date varchar,
   invoice_date varchar,
   aging_start_date varchar,
   original_currency varchar,
   invoice_description varchar,
   sales_owner varchar,
   invoice_number varchar,
   customer_name varchar,
   aging_months varchar,
   exchange_rate varchar,
   vat varchar,
   sap_code varchar,
   payment_transaction_code varchar,
   contract_number varchar,
   invoice_amount_before_tax varchar,
   initial_receivable_amount varchar,
   contract_value varchar,
   note varchar,
   company varchar,
   collected_and_offset_receivable varchar,
   outstanding_receivable varchar,
   not_due_yet varchar,
   overdue_over_12_months varchar,
   overdue_6_to_12_months varchar,
   overdue_3_to_6_months varchar,
   overdue_under_3_months varchar,
   overdue_under_1_month varchar,
   source_file varchar,
   crawled_at_ts bigint,
   report_date_ts bigint,
   invoice_date_ts bigint,
   aging_start_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.dim_product_category_code definition

CREATE TABLE hive.finance_raw.dim_product_category_code (
   category_code varchar,
   product_category varchar,
   product_erp_name varchar,
   item_type varchar,
   source_file varchar,
   crawled_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.finance_plan definition

CREATE TABLE hive.finance_raw.finance_plan (
   plan_date varchar,
   company varchar,
   plan_viettel_group varchar,
   plan_must varchar,
   plan_nice varchar,
   segment varchar,
   subgroup varchar,
   source_file varchar,
   crawled_at_ts bigint,
   plan_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.financial_ratio definition

CREATE TABLE hive.finance_raw.financial_ratio (
   report_date varchar,
   description varchar,
   value varchar,
   source_file varchar,
   crawled_at_ts bigint,
   report_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.product_revenue_plan definition

CREATE TABLE hive.finance_raw.product_revenue_plan (
   product_group varchar,
   product_code varchar,
   product_name varchar,
   year bigint,
   month bigint,
   period varchar,
   currency_code varchar,
   snapshot_at varchar,
   snapshot_version bigint,
   must bigint,
   nice bigint,
   crawled_at_ts bigint,
   snapshot_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.product_revenue_plan_2026 definition

CREATE TABLE hive.finance_raw.product_revenue_plan_2026 (
   customer_group_name varchar,
   customer_subgroup_name varchar,
   group_name varchar,
   subgroup_name varchar,
   service_code varchar,
   service_name varchar,
   priority_must varchar,
   priority_should varchar,
   priority_nice varchar,
   amount_unit varchar,
   report_month varchar,
   report_year varchar,
   report_date varchar,
   source_file varchar,
   crawled_at_ts bigint,
   report_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.production_cost_allocation definition

CREATE TABLE hive.finance_raw.production_cost_allocation (
   date_key varchar,
   product_service_name varchar,
   product_service_code varchar,
   expense_code varchar,
   product_service_expense_category varchar,
   data_type varchar,
   market varchar,
   amount_million_vnd varchar,
   source_file varchar,
   crawled_at_ts bigint,
   date_key_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.provisional_revenue definition

CREATE TABLE hive.finance_raw.provisional_revenue (
   share varchar,
   report_date varchar,
   revenue varchar,
   customer_name varchar,
   old_category varchar,
   payment_request_description varchar,
   segment_level_3 varchar,
   category_code varchar,
   revenue_group varchar,
   product_type varchar,
   is_soc varchar,
   product_service_group varchar,
   territory varchar,
   vat_amount varchar,
   account_manager varchar,
   month varchar,
   sales_channel varchar,
   department varchar,
   presale_owner varchar,
   payment_stage varchar,
   share_2 varchar,
   revenue_2 varchar,
   invoice_issue_date varchar,
   customer_segment varchar,
   revenue_recognition_type varchar,
   customer_group varchar,
   revenue_allocated_unit varchar,
   source_file varchar,
   crawled_at_ts bigint,
   report_date_ts bigint,
   invoice_issue_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.sales_revenue definition

CREATE TABLE hive.finance_raw.sales_revenue (
   invoice_date varchar,
   invoice_month varchar,
   invoice_type varchar,
   billing_description varchar,
   revenue_amount varchar,
   revenue_share_amount double,
   final_revenue_amount varchar,
   vat_amount varchar,
   gross_amount varchar,
   revenue_recognition_type varchar,
   service_category varchar,
   customer_scope varchar,
   customer_type varchar,
   customer_channel varchar,
   customer_name varchar,
   customer_segment varchar,
   department varchar,
   customer_group varchar,
   account_manager varchar,
   presale double,
   service_name varchar,
   revenue_type varchar,
   soc_type varchar,
   mss_shared_revenue varchar,
   market_scope varchar,
   service_code varchar,
   region varchar,
   note double,
   current_account_manager varchar,
   is_vvip_customer double,
   is_master_contract double,
   source_file varchar,
   crawled_at_ts bigint,
   invoice_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.sales_revenue_share_all definition

CREATE TABLE hive.finance_raw.sales_revenue_share_all (
   invoice_date varchar,
   invoice_month varchar,
   invoice_type varchar,
   billing_description varchar,
   revenue_amount varchar,
   revenue_share_amount double,
   final_revenue_amount varchar,
   vat_amount varchar,
   gross_amount varchar,
   revenue_recognition_type varchar,
   service_category varchar,
   customer_scope varchar,
   customer_type varchar,
   customer_channel varchar,
   customer_name varchar,
   customer_segment varchar,
   department varchar,
   customer_group varchar,
   account_manager varchar,
   presale double,
   service_name varchar,
   revenue_type varchar,
   soc_type varchar,
   mss_shared_revenue varchar,
   market_scope varchar,
   service_code varchar,
   region varchar,
   note double,
   current_account_manager varchar,
   is_vvip_customer double,
   is_master_contract double,
   note__af varchar,
   source_file varchar,
   note__ac varchar,
   crawled_at_ts bigint,
   invoice_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.finance_raw.sales_revenue_share_soc_product_category definition

CREATE TABLE hive.finance_raw.sales_revenue_share_soc_product_category (
   invoice_date varchar,
   invoice_month varchar,
   invoice_type varchar,
   billing_description varchar,
   revenue_amount varchar,
   revenue_share_amount double,
   final_revenue_amount varchar,
   vat_amount varchar,
   gross_amount varchar,
   revenue_recognition_type varchar,
   service_category varchar,
   customer_scope varchar,
   customer_type varchar,
   customer_channel varchar,
   customer_name varchar,
   customer_segment varchar,
   department varchar,
   customer_group varchar,
   account_manager varchar,
   presale double,
   service_name varchar,
   revenue_type varchar,
   soc_type varchar,
   mss_shared_revenue varchar,
   market_scope varchar,
   service_code varchar,
   region varchar,
   note double,
   current_account_manager varchar,
   is_vvip_customer double,
   is_master_contract double,
   note__af varchar,
   source_file varchar,
   note__ac varchar,
   crawled_at_ts bigint,
   invoice_date_ts bigint
)
WITH (
   format = 'PARQUET'
);

-- hive.finance_raw.actual_cost definition

CREATE TABLE hive.finance_raw.actual_cost (
             report_date varchar,
  report_year varchar,
  report_month varchar,
  old_category varchar,
  category_code varchar,
  cost_group varchar,
  base_currency_amount varchar,
  currency_code varchar,
  territory_name varchar,
  product_category varchar,
  unit_level_1 varchar,
  source_file varchar,
  crawled_at_ts BIGINT,
  report_date_ts BIGINT
)
WITH (
   format = 'PARQUET'
);

