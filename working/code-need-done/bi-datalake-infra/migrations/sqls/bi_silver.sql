SHOW SCHEMAS FROM hive;
CREATE SCHEMA hive.bi_silver 
with (location='s3a://bi-silver/');


-- hive.bi_silver.company_contacts definition

CREATE TABLE hive.bi_silver.company_contacts (
   company_id varchar,
   company_name varchar,
   source_system varchar,
   contact_id varchar,
   contact_name varchar,
   first_name varchar,
   last_name varchar,
   email varchar,
   email_domain varchar,
   created_at timestamp(3),
   updated_at timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.contract_collected_invoices definition

CREATE TABLE hive.bi_silver.contract_collected_invoices (
   idd varchar,
   sales_channel varchar,
   transaction_code varchar,
   invoice_number varchar,
   contract_num varchar,
   customer_code varchar,
   contract_description varchar,
   company_name varchar,
   original_currency varchar,
   initial_receivable_amount varchar,
   total_received_amount varchar,
   remaining_balance_amount varchar,
   invoice_date varchar,
   payment_deadline varchar,
   payment_date varchar,
   overdue_months_numeric double,
   overdue_status_enum varchar,
   am_username varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_contract_allocations definition

CREATE TABLE hive.bi_silver.crm_contract_allocations (
   deal_id varchar,
   deal_name varchar,
   deal_stage_name varchar,
   contract_id varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   customer_group varchar,
   company_id varchar,
   sales_account_id bigint,
   company_name varchar,
   company_alias varchar,
   currency_code varchar,
   am_username varchar,
   due_date date,
   fac_date date,
   pricebook_id varchar,
   product_category_index varchar,
   product_description varchar,
   product_category varchar,
   product_type varchar,
   deployment_type varchar,
   is_recurring boolean,
   product_index varchar,
   territory_name varchar,
   product_total_value decimal(18, 2),
   sales_performance_value decimal(18, 2),
   period_value decimal(18, 2),
   period_number integer,
   period_name varchar,
   invoice_activation_date date,
   created_at timestamp(3),
   updated_at timestamp(3),
   closed_date timestamp(3),
   days_since_last_update integer,
   sale_admin varchar,
   first_payment_date date,
   id varchar,
   period_index integer
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_contracts definition

CREATE TABLE hive.bi_silver.crm_contracts (
   id varchar,
   name varchar,
   user_am_id varchar,
   deal_id varchar,
   contract_number varchar,
   contract_type varchar,
   signing_method varchar,
   signing_method_code varchar,
   sale_account_id bigint,
   company_alias varchar,
   customer_name varchar,
   tax_code varchar,
   customer_group_vi varchar,
   segment_l1 varchar,
   customer_segment_l1 varchar,
   segment_l2 varchar,
   customer_segment_l2 varchar,
   segment_l3 varchar,
   customer_segment_l3 varchar,
   status varchar,
   status_code varchar,
   status_vi varchar,
   customer_group varchar,
   is_vip_customer integer,
   is_enterprise_customer integer,
   is_state_owned integer,
   is_private_enterprise integer,
   is_banking_group integer,
   is_international_client integer,
   is_internal_client integer,
   sign_date timestamp(3),
   fac_date timestamp(3),
   duration double,
   expire_date timestamp(3),
   usd_exchange_rate double,
   currency_code varchar,
   contract_amount decimal(18, 2),
   vat bigint,
   vcs_contract_amount decimal(18, 2),
   viettel_contract_amount decimal(18, 2),
   partner_id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_deal_interested_products definition

CREATE TABLE hive.bi_silver.crm_deal_interested_products (
   deal_id varchar,
   created_at timestamp(3),
   interested_product_category varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_deal_quotation_products definition

CREATE TABLE hive.bi_silver.crm_deal_quotation_products (
   deal_id varchar,
   quotation_id varchar,
   product_line_no integer,
   product_id varchar,
   quotation_created_at timestamp(3),
   quotation_status varchar,
   quotation_lang varchar,
   currency_code varchar,
   global_discount decimal(18, 2),
   global_discount_type varchar,
   product_name varchar,
   base_price decimal(18, 2),
   quantitative integer,
   unit varchar,
   product_unit varchar,
   duration integer,
   package varchar,
   vat integer,
   discount decimal(18, 2),
   discount_type varchar,
   final_total_amount decimal(18, 2),
   base_total_amount decimal(18, 2),
   id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_deal_quotations definition

CREATE TABLE hive.bi_silver.crm_deal_quotations (
   deal_id varchar,
   quotation_id varchar,
   created_at timestamp(3),
   status varchar,
   tlang varchar,
   currency_code varchar,
   global_discount decimal(18, 2),
   global_discount_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_deal_reasons definition

CREATE TABLE hive.bi_silver.crm_deal_reasons (
   id varchar,
   reason_name varchar,
   position bigint,
   is_partial boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_deals definition

CREATE TABLE hive.bi_silver.crm_deals (
   deal_id varchar,
   deal_name varchar,
   currency_amount decimal(18, 2),
   vnd_amount decimal(18, 2),
   expected_close_date timestamp(3),
   closed_date timestamp(3),
   stage_updated_time timestamp(3),
   days_to_close integer,
   days_since_last_update integer,
   is_deal_locked boolean,
   usd_to_vnd double,
   get_live_date timestamp(3),
   periodicity varchar,
   is_select_viettel boolean,
   has_budget boolean,
   company_id varchar,
   company_alias varchar,
   budget_amount decimal(18, 2),
   customer_group varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   channel varchar,
   opportunity_weight_pct double,
   bidding_required boolean,
   presales_name varchar,
   project_manager varchar,
   sales_admin varchar,
   partner_id varchar,
   contract_id varchar,
   initial_source varchar,
   warm_up_source varchar,
   territory_name varchar,
   contract_duration_month bigint,
   is_created_contract boolean,
   expire_date timestamp(3),
   company_name varchar,
   company_address varchar,
   company_province varchar,
   company_country varchar,
   company_email varchar,
   company_phone varchar,
   company_contact varchar,
   currency_code varchar,
   quotation_status varchar,
   fac_date timestamp(3),
   am_username varchar,
   check_change boolean,
   probability bigint,
   updated_at timestamp(3),
   created_at timestamp(3),
   deal_stage_id varchar,
   deal_status varchar,
   deal_payment_status_id varchar,
   age bigint,
   recent_note varchar,
   next_scheduled_activity_time timestamp(3),
   last_assigned_at timestamp(3),
   last_contacted_activity_status varchar,
   last_contacted_activity_time timestamp(3),
   expected_deal_value decimal(18, 2),
   signing_delay_days integer,
   am_user_id varchar,
   sales_account_id varchar,
   deal_type_id varchar,
   deal_reason_id varchar,
   currency_id varchar,
   vnd_to_usd double,
   deal_payment_status varchar,
   deal_stage_name varchar,
   sign_date timestamp(3),
   deal_stage_code varchar,
   due_date timestamp(3),
   is_signed integer,
   probability_bucket varchar,
   contract_type varchar,
   deal_reason varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_partners definition

CREATE TABLE hive.bi_silver.crm_partners (
   id varchar,
   partner_name varchar,
   document_number varchar,
   company_alias varchar,
   partner_address varchar,
   country varchar,
   contact_name varchar,
   contact_position varchar,
   contact_mobile varchar,
   contact_email varchar,
   effective_date timestamp(3),
   expiration_date timestamp(3),
   partner_type varchar,
   document_type varchar,
   partner_status varchar,
   partner_type_vi varchar,
   document_format_vi varchar,
   partner_status_vi varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_payment_forecast definition

CREATE TABLE hive.bi_silver.crm_payment_forecast (
   payment_date timestamp(3),
   deal_id varchar,
   update_at timestamp(3),
   currency_amount decimal(18, 2),
   vnd_amount decimal(18, 2),
   is_actual boolean,
   due_date timestamp(3),
   product_type varchar,
   deployment_type varchar,
   product_category varchar,
   is_recurring boolean,
   territory_name varchar,
   product_index varchar,
   customer_id varchar,
   customer_name varchar,
   company_alias varchar,
   customer_group varchar,
   product_category_index varchar,
   currency_code varchar,
   deal_stage_name varchar,
   am_username varchar,
   contract_id varchar,
   payment_index bigint,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   sales_admin_username varchar,
   contract_allocation_id varchar
)
WITH (
   format = 'PARQUET'
);

-- hive.bi_silver.crm_allocated_revenue definition

CREATE TABLE hive.bi_silver.crm_allocated_revenue (
   report_date timestamp(3),
   report_year integer,
   report_month integer,
   product_category_code varchar,
   product_category varchar,
   product_growth_type varchar,
   product_service_group varchar,
   revenue_amount decimal(18, 2),
   company_alias varchar,
   revenue_type varchar,
   channel_name varchar,
   is_soc boolean,
   department_alias varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   revenue_group varchar,
   vat integer,
   payment_stage varchar,
   am_username varchar,
   presale_name varchar,
   currency_code varchar,
   business_unit_level_1 varchar,
   territory_name varchar,
   deal_id varchar,
   update_at timestamp(3),
   currency_amount decimal(18, 2),
   is_actual boolean,
   due_date timestamp(3),
   deployment_type varchar,
   pricebook_id varchar,
   is_recurring boolean,
   product_index varchar,
   customer_id varchar,
   customer_name varchar,
   customer_group varchar,
   product_category_index varchar,
   contract_id varchar,
   payment_index bigint,
   contract_allocation_id varchar
)
WITH (
   format = 'PARQUET'
);

-- hive.bi_silver.crm_pricebook definition

CREATE TABLE hive.bi_silver.crm_pricebook (
   product_id bigint,
   product_name varchar,
   product_description varchar,
   product_group varchar,
   product_sub_group varchar,
   category_code varchar,
   product_category varchar,
   sku varchar,
   product_version varchar,
   item_type varchar,
   is_active boolean,
   product_type varchar,
   sub_type varchar,
   product_license varchar,
   price_type varchar,
   package varchar,
   price decimal(18, 2),
   currency_code varchar,
   price_min decimal(18, 2),
   price_max decimal(18, 2),
   price_unit varchar,
   is_quantity_based boolean,
   is_related_csmp boolean,
   csmp_discount decimal(18, 2),
   csmp_discount_silver decimal(18, 2),
   csmp_discount_gold decimal(18, 2),
   csmp_discount_diamond decimal(18, 2),
   created_at date,
   updated_at date,
   item_code varchar,
   idd varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_product_category definition

CREATE TABLE hive.bi_silver.crm_product_category (
   category_code varchar,
   product_category varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_product_tree definition

CREATE TABLE hive.bi_silver.crm_product_tree (
   product_group varchar,
   product_sub_group varchar,
   category_code varchar,
   product_category varchar,
   sku varchar,
   version varchar,
   item_type varchar,
   license varchar,
   price_type varchar,
   unit varchar,
   item_code varchar,
   idd varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_product_tree_item_code definition

CREATE TABLE hive.bi_silver.crm_product_tree_item_code (
   idd varchar,
   item_code varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_sales_accounts definition

CREATE TABLE hive.bi_silver.crm_sales_accounts (
   id varchar,
   is_partner boolean,
   name varchar,
   address varchar,
   city varchar,
   state varchar,
   zipcode varchar,
   country varchar,
   number_of_employees bigint,
   annual_revenue varchar,
   website varchar,
   am_username varchar,
   sales_admin varchar,
   phone varchar,
   open_deals_amount decimal(18, 2),
   open_deals_count bigint,
   won_deals_amount decimal(18, 2),
   won_deals_count bigint,
   tax_code varchar,
   company_id varchar,
   company_alias varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   incorporation_date date,
   initial_source varchar,
   warm_up_source varchar,
   using_soc varchar,
   vcs_soc_others varchar,
   soc_brand varchar,
   service_level varchar,
   created_at timestamp(3),
   updated_at timestamp(3),
   days_since_update integer,
   parent_sales_account_id varchar,
   recent_note varchar,
   last_contacted_via_sales_activity date,
   last_contacted_sales_activity_mode varchar,
   last_assigned_at date,
   renewal_date date,
   business_type varchar,
   industry_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_users definition

CREATE TABLE hive.bi_silver.crm_users (
   id bigint,
   email varchar,
   display_name varchar,
   is_active boolean,
   job_title varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.cx_cso_support_tickets definition

CREATE TABLE hive.bi_silver.cx_cso_support_tickets (
   ticket_id varchar,
   subject varchar,
   customer_group varchar,
   status_name varchar,
   priority_level varchar,
   created_at timestamp(3),
   updated_at timestamp(3),
   first_responded_at timestamp(3),
   resolved_at timestamp(3),
   closed_at timestamp(3),
   call_reminder varchar,
   issue_category varchar,
   is_vcs boolean,
   l1 boolean,
   l2 boolean,
   l3 boolean,
   l4 boolean,
   is_duplicated_ticket boolean,
   is_reopened_by_cx boolean,
   assigned_to varchar,
   action_program varchar,
   company_alias varchar,
   customer_satisfaction_rating bigint,
   customer_entry_channel varchar,
   customer_respond_date timestamp(3),
   communication_effectiveness varchar,
   imported_ticket_date timestamp(3),
   imported_ticket_due_date timestamp(3),
   incident_root_cause varchar,
   issues_type varchar,
   note varchar,
   number_of_due_date_changes bigint,
   old_due_date timestamp(3),
   is_one_day_before_due boolean,
   is_one_hour_before_due boolean,
   reason varchar,
   related_ticket varchar,
   is_reminder_update boolean,
   is_notification boolean,
   is_overdue boolean,
   response_time_minutes double,
   sentiment varchar,
   severity_level varchar,
   spam_type varchar,
   support_category varchar,
   is_third_four_time boolean,
   ttr_overdue varchar,
   urgency_level varchar,
   resolution_time_minutes double,
   first_response_time_minutes double,
   l1_time_actual_minutes double,
   l1_time_allowed_minutes double,
   l1_violated boolean,
   l2_time_actual_minutes double,
   l2_time_allowed_minutes double,
   l2_violated boolean,
   l3_time_actual_minutes double,
   l3_time_allowed_minutes double,
   l3_violated boolean,
   l4_time_actual_minutes double,
   l4_time_allowed_minutes double,
   l4_violated boolean,
   time_to_response_minutes double,
   assigned_agent_stage varchar,
   violated_level integer,
   requester_name varchar,
   company_name varchar,
   agent_name varchar,
   resolution_time_in_business_hours double,
   agent_reply_count integer,
   first_response_date timestamp(3),
   due_date_change_count bigint,
   tickets_first_responded_within_sla varchar,
   tickets_resolved_within_sla varchar,
   ttr_time_minutes double,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   product_category_code varchar,
   product_category varchar,
   product_item_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.cx_cso_ticket_tags definition

CREATE TABLE hive.bi_silver.cx_cso_ticket_tags (
   ticket_id varchar,
   tag varchar,
   created_at timestamp(3),
   updated_at timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.cx_sur_question_answer_choices definition

CREATE TABLE hive.bi_silver.cx_sur_question_answer_choices (
   survey_id varchar,
   question_id varchar,
   question_type varchar,
   question_clean varchar,
   answer_choice_id varchar,
   answer_choice_content varchar,
   kpi_type varchar,
   journey varchar,
   customer_touchpoint varchar,
   product_category varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.cx_sur_question_response definition

CREATE TABLE hive.bi_silver.cx_sur_question_response (
   collected_date timestamp(3),
   survey_id varchar,
   response_id varchar,
   respondent_uuid varchar,
   cso_ticket_id varchar,
   question_id varchar,
   question_type varchar,
   choice_id varchar,
   answer_string varchar,
   answer_tag varchar,
   comment varchar,
   choice_content varchar,
   item_content varchar,
   answer_number double,
   item_score_value double,
   item_score_label varchar,
   question_clean varchar,
   kpi_type varchar,
   journey varchar,
   customer_touchpoint varchar,
   product_category varchar,
   kpi_group varchar,
   max_nps_score varchar,
   max_csat_score varchar,
   max_ces_score varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.cx_sur_surveys definition

CREATE TABLE hive.bi_silver.cx_sur_surveys (
   survey_id varchar,
   survey_type varchar,
   survey_name varchar,
   folder_name varchar,
   created_at timestamp(3),
   enabled boolean,
   journey varchar,
   customer_touchpoint varchar,
   product_category varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.dim_customer_segment_l1 definition

CREATE TABLE hive.bi_silver.dim_customer_segment_l1 (
   segment_l1 varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.dim_date definition

CREATE TABLE hive.bi_silver.dim_date (
   date_ts timestamp(3),
   date_key integer,
   date_year integer,
   date_quarter integer,
   date_month integer,
   date_week integer,
   date_day integer,
   date_day_of_week integer,
   date_day_name varchar,
   date_month_name varchar,
   date_year_month integer,
   date_year_week integer,
   date_is_weekend boolean,
   date_is_month_end boolean,
   date_is_quarter_end boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.dim_product_category definition

CREATE TABLE hive.bi_silver.dim_product_category (
   product_category varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.dim_territory_name definition

CREATE TABLE hive.bi_silver.dim_territory_name (
   territory_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.dim_unit_level_1 definition

CREATE TABLE hive.bi_silver.dim_unit_level_1 (
   unit_level_1 varchar COMMENT 'Organization level N'
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_actual_cost definition

CREATE TABLE hive.bi_silver.finance_actual_cost (
   report_date timestamp(3),
   report_year integer,
   report_month integer,
   old_category varchar,
   product_category_code varchar,
   cost_group varchar,
   base_currency_amount decimal(18, 2),
   currency_code varchar,
   territory_name varchar,
   product_category varchar,
   business_unit_level_1 varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_allocated_revenue definition

CREATE TABLE hive.bi_silver.finance_allocated_revenue (
   report_date timestamp(3),
   report_year integer,
   report_month integer,
   product_category_code varchar,
   product_category varchar,
   product_growth_type varchar,
   product_service_group varchar,
   revenue_amount decimal(18, 2),
   company_alias varchar,
   revenue_type varchar,
   channel_name varchar,
   is_soc boolean,
   department_alias varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   revenue_group varchar,
   vat integer,
   payment_stage varchar,
   am_username varchar,
   presale_name varchar,
   currency_code varchar,
   business_unit_level_1 varchar,
   territory_name varchar,
   deal_id varchar,
   update_at timestamp(3),
   currency_amount decimal(18, 2),
   is_actual boolean,
   due_date timestamp(3),
   deployment_type varchar,
   pricebook_id varchar,
   is_recurring boolean,
   product_index varchar,
   customer_id varchar,
   customer_name varchar,
   customer_group varchar,
   product_category_index varchar,
   contract_id varchar,
   payment_index bigint,
   contract_allocation_id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_contract_collected_invoices definition

CREATE TABLE hive.bi_silver.finance_contract_collected_invoices (
   idd varchar,
   sales_channel varchar,
   transaction_code varchar,
   invoice_number varchar,
   contract_num varchar,
   customer_code varchar,
   contract_description varchar,
   company_name varchar,
   original_amount decimal(18, 3),
   initial_receivable_amount decimal(18, 3),
   total_received_amount decimal(18, 3),
   remaining_balance_amount decimal(18, 3),
   invoice_date timestamp(3),
   payment_deadline timestamp(3),
   payment_date timestamp(3),
   overdue_months_numeric double,
   overdue_status_enum varchar,
   am_username varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_cost_plan definition

CREATE TABLE hive.bi_silver.finance_cost_plan (
   plan_date timestamp(3),
   plan_year integer,
   plan_month integer,
   cost_code varchar,
   cost_group varchar,
   expected_cost_value decimal(18, 2),
   currency_code varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_fact_ratios definition

CREATE TABLE hive.bi_silver.finance_fact_ratios (
   report_date timestamp(3),
   metric_code varchar,
   metric_name varchar,
   metric_value double
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_product_revenue_plan definition

CREATE TABLE hive.bi_silver.finance_product_revenue_plan (
   plan_date timestamp(3),
   plan_year bigint,
   plan_month bigint,
   plan_must_amount decimal(18, 2),
   plan_should_amount decimal(18, 2),
   plan_nice_amount decimal(18, 2),
   product_group varchar,
   product_category_code varchar,
   product_category varchar,
   product_name varchar,
   business_group_name varchar,
   sub_group_name varchar,
   currency_code varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_production_cost_allocations definition

CREATE TABLE hive.bi_silver.finance_production_cost_allocations (
   report_date timestamp(3),
   report_year integer,
   report_month integer,
   old_category varchar,
   old_product_category varchar,
   product_category_code varchar,
   cost_group varchar,
   base_currency_amount decimal(18, 2),
   currency_code varchar,
   territory_name varchar,
   expense_code varchar,
   market_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.finance_revenue_plan definition

CREATE TABLE hive.bi_silver.finance_revenue_plan (
   plan_date timestamp(3),
   plan_year integer,
   plan_month integer,
   plan_viettel_group_amount decimal(18, 2),
   plan_must_amount decimal(18, 2),
   plan_nice_amount decimal(18, 2),
   segment_l1_alias varchar,
   currency_code varchar,
   segment_l1 varchar,
   customer_segment_l1 varchar,
   segment_l2 varchar,
   customer_segment_l2 varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.giang_test_masterplan definition

CREATE TABLE hive.bi_silver.giang_test_masterplan (
   plan_date date,
   category_code varchar,
   product_category varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   plan_viettel_group double,
   plan_must double,
   plan_should double,
   plan_nice double
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.hr_employee_headcount definition

CREATE TABLE hive.bi_silver.hr_employee_headcount (
   headcount_plan varchar,
   headcount_status varchar,
   division_n varchar,
   unit_n_1 varchar,
   department_n_2 varchar,
   department_n_3 varchar,
   role_base varchar,
   specialization varchar,
   required_level varchar,
   position_status varchar,
   full_name varchar,
   employee_code varchar,
   employee_object varchar,
   contract_type varchar,
   hire_date_vcs date,
   level varchar,
   region varchar,
   management_level varchar,
   branch varchar,
   business_email varchar,
   job_framework_position varchar,
   job_framework_position_2 varchar,
   service_group varchar,
   service_group_new varchar,
   is_ai_group integer,
   is_ai_org integer,
   is_philippines_japan_market integer,
   is_domestic_business integer,
   is_international_business integer,
   is_rnd integer,
   is_indirect_group integer,
   is_business_support integer
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.hr_employee_onboard definition

CREATE TABLE hive.bi_silver.hr_employee_onboard (
   employee_code varchar,
   full_name varchar,
   current_status varchar,
   recruitment_status varchar,
   employee_type varchar,
   contract_company_independent varchar,
   branch varchar,
   division varchar,
   unit_level_1 varchar,
   business_unit_level_1 varchar,
   department_level_2 varchar,
   role_base varchar,
   job_level varchar,
   technical_grade_band varchar,
   technical_grade_level integer,
   management_level varchar,
   hire_date date,
   hire_date_viettel date,
   termination_date date,
   business_email varchar,
   email_nametag varchar,
   date_of_birth date,
   gender varchar,
   pr_q1_2024 double,
   pr_q2_2024 double,
   pr_q3_2024 double,
   pr_q4_2024 double,
   pr_q1_2025 double,
   pr_q2_2025 double,
   pr_q3_2025 double,
   achievement_title_2024 varchar,
   talent_group varchar,
   performance_level varchar,
   potential_level varchar,
   termination_status varchar,
   termination_reason_1 varchar,
   termination_reason_2 varchar,
   termination_detail_reason varchar,
   next_workplace double,
   is_new_hire varchar,
   age integer,
   age_group varchar,
   department_level_3 varchar,
   key_core_group integer,
   is_key_employee integer,
   employee_criticality_level varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.hr_employee_resigned definition

CREATE TABLE hive.bi_silver.hr_employee_resigned (
   employee_code varchar,
   full_name varchar,
   current_status varchar,
   business_email varchar,
   email_nametag varchar,
   recruitment_status varchar,
   employee_type varchar,
   contract_company_independent varchar,
   branch varchar,
   division varchar,
   unit_level_1 varchar,
   business_unit_level_1 varchar,
   department_level_2 varchar,
   department_n_3 varchar,
   role_base varchar,
   job_level varchar,
   region varchar,
   management_level varchar,
   hire_date date,
   hire_date_vcs date,
   termination_date date,
   date_of_birth date,
   gender varchar,
   seniority_group varchar,
   pr_q1_2024 double,
   pr_q2_2024 double,
   pr_q3_2024 double,
   pr_q4_2024 double,
   pr_q1_2025 double,
   pr_q2_2025 double,
   achievement_2024 varchar,
   talent_9box_group varchar,
   performance_level varchar,
   potential_level varchar,
   leave_status varchar,
   resignation_reason_level_1 varchar,
   resignation_reason_level_2 varchar,
   resignation_reason_detail varchar,
   next_workplace_score double,
   employment_status varchar,
   termination_reason varchar,
   is_key_employee integer,
   is_high_potential integer
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.jira_msn definition

CREATE TABLE hive.bi_silver.jira_msn (
   key varchar,
   start_date date,
   summary varchar,
   issuetype varchar,
   status varchar,
   assignee varchar,
   projectkey varchar,
   department_center varchar,
   task_type varchar,
   duedate date,
   created date,
   updated date
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.jira_task_operation definition

CREATE TABLE hive.bi_silver.jira_task_operation (
   key varchar,
   start_date date,
   due_date date,
   summary varchar,
   issue_type varchar,
   priority varchar,
   status varchar,
   assignor varchar,
   work_group varchar,
   department_center varchar,
   assignee varchar,
   created date,
   updated date
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.noc_entities definition

CREATE TABLE hive.bi_silver.noc_entities (
   customer varchar,
   project varchar,
   host varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.noc_metrics definition

CREATE TABLE hive.bi_silver.noc_metrics (
   metric_name varchar,
   agg_type varchar,
   customer varchar,
   project varchar,
   host varchar,
   metric_value double,
   metric_ts bigint,
   metric_time varchar,
   dt date
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_company_contacts source

CREATE VIEW hive.bi_silver.crm_company_contacts SECURITY DEFINER AS
SELECT
  company_id
, upper(company_name) company_name
, created_at
, updated_at
FROM
  bi_silver.company_contacts;


-- hive.bi_silver.crm_deal_interested_product_stats source

CREATE VIEW hive.bi_silver.crm_deal_interested_product_stats SECURITY DEFINER AS
SELECT
  UPPER(interested_product_category) product_category
, count(*) number_of_deals
FROM
  bi_silver.crm_deal_interested_products
GROUP BY 1;


-- hive.bi_silver.finance_daily_business_metrics source

CREATE VIEW hive.bi_silver.finance_daily_business_metrics SECURITY DEFINER AS
WITH
  actual_cost AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , business_unit_level_1 business_unit_level_1
   , territory_name
   , SUM(base_currency_amount) actual_cost_amount
   FROM
     hive.bi_silver.finance_actual_cost
   GROUP BY CAST(report_date AS DATE), product_category, business_unit_level_1, territory_name
) 
, cash_collection AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , business_unit_level_1 business_unit_level_1
   , territory_name territory_name
   , SUM(revenue_amount) allocated_revenue_amount
   FROM
     hive.bi_silver.crm_allocated_revenue
   GROUP BY CAST(report_date AS DATE), product_category, business_unit_level_1, territory_name
) 
, sales_revenue AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , null business_unit_level_1
   , territory_name territory_name
   , SUM(revenue_amount) sales_revenue_amount
   FROM
     hive.bi_silver.crm_allocated_revenue
   GROUP BY CAST(report_date AS DATE), product_category, territory_name
) 
, revenue_plan AS (
   SELECT
     CAST(plan_date AS DATE) report_date
   , product_category
   , null business_unit_level_1
   , null territory_name
   , SUM(plan_must_amount) planned_revenue_must_amount
   , SUM(plan_nice_amount) planned_revenue_nice_amount
   FROM
     hive.bi_silver.finance_product_revenue_plan
   GROUP BY CAST(plan_date AS DATE), product_category
) 
, fact_union AS (
   SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , actual_cost_amount
   , 0 allocated_revenue_amount
   , 0 sales_revenue_amount
   , 0 planned_revenue_must_amount
   , 0 planned_revenue_nice_amount
   FROM
     actual_cost
UNION ALL    SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , 0
   , allocated_revenue_amount
   , 0
   , 0
   , 0
   FROM
     cash_collection
UNION ALL    SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , 0
   , 0
   , sales_revenue_amount
   , 0
   , 0
   FROM
     sales_revenue
UNION ALL    SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , 0
   , 0
   , 0
   , planned_revenue_must_amount
   , planned_revenue_nice_amount
   FROM
     revenue_plan
) 
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(COALESCE(product_category, 'Khác')) product_category
, UPPER(COALESCE(business_unit_level_1, 'Khác')) business_unit_level_1
, UPPER(COALESCE(territory_name, 'Khác')) territory_name
, CAST(SUM(actual_cost_amount) AS BIGINT) actual_cost_amount
, CAST(SUM(allocated_revenue_amount) AS BIGINT) allocated_revenue_amount
, CAST(SUM(allocated_revenue_amount) AS BIGINT) cash_collection_amount
, CAST(SUM(sales_revenue_amount) AS BIGINT) sales_revenue_amount
, CAST(SUM(planned_revenue_must_amount) AS BIGINT) planned_revenue_must_amount
, CAST(SUM(planned_revenue_nice_amount) AS BIGINT) planned_revenue_nice_amount
, CAST((SUM(sales_revenue_amount) - SUM(actual_cost_amount)) AS BIGINT) gross_profit_amount
, CAST((SUM(allocated_revenue_amount) - SUM(planned_revenue_must_amount)) AS BIGINT) revenue_vs_plan_gap_amount
FROM
  fact_union
GROUP BY 1, 2, 3, 4;


-- hive.bi_silver.finance_metrics source

CREATE VIEW hive.bi_silver.finance_metrics SECURITY DEFINER AS
WITH
  rev_agg AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category_code
   , product_category
   , business_unit_level_1
   , CAST(SUM(revenue_amount) AS BIGINT) total_revenue
   , 0 fixed_asset_depreciation_cost
   , 0 direct_labor_os_cost
   , 0 production_infra_cost
   , 0 business_os_cost
   , 0 total_cost
   FROM
     hive.bi_silver.crm_allocated_revenue
   GROUP BY 1, 2, 3, 4
) 
, cost_agg AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category_code
   , product_category
   , business_unit_level_1
   , 0 total_revenue
   , CAST(SUM((CASE WHEN (cost_group = 'Chi phí khấu hao tài sản cố định') THEN base_currency_amount ELSE 0 END)) AS BIGINT) fixed_asset_depreciation_cost
   , CAST(SUM((CASE WHEN (cost_group IN ('Chi phí Nhân công & OS trực tiếp', 'Chi phí Nhân công, outsource trực tiếp')) THEN base_currency_amount ELSE 0 END)) AS BIGINT) direct_labor_os_cost
   , CAST(SUM((CASE WHEN (cost_group IN ('Chi phí tài nguyên, CCDC phục vụ sản xuất', 'Chi phí hạ tầng, máy chủ, CCDC sản xuất')) THEN base_currency_amount ELSE 0 END)) AS BIGINT) production_infra_cost
   , CAST(SUM((CASE WHEN (cost_group = 'Chi phí OS kinh doanh') THEN base_currency_amount ELSE 0 END)) AS BIGINT) business_os_cost
   , CAST(SUM(base_currency_amount) AS BIGINT) total_cost
   FROM
     hive.bi_silver.finance_actual_cost
   GROUP BY 1, 2, 3, 4
) 
, union_all_join AS (
   SELECT *
   FROM
     rev_agg
UNION ALL    SELECT *
   FROM
     cost_agg
) 
, final_join AS (
   SELECT
     report_date
   , product_category_code
   , product_category
   , business_unit_level_1
   , sum(total_revenue) total_revenue
   , sum(total_cost) total_cost
   , sum(direct_labor_os_cost) direct_labor_os_cost
   , sum(production_infra_cost) production_infra_cost
   , sum(fixed_asset_depreciation_cost) fixed_asset_depreciation_cost
   , sum(business_os_cost) business_os_cost
   FROM
     union_all_join
   GROUP BY 1, 2, 3, 4
) 
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(product_category_code) product_category_code
, UPPER(product_category) product_category
, UPPER(business_unit_level_1) business_unit_level_1
, total_revenue
, total_cost
, (total_revenue - (direct_labor_os_cost + production_infra_cost)) gross_profit
, ((total_revenue - total_cost) + fixed_asset_depreciation_cost) ebitda
, fixed_asset_depreciation_cost
, direct_labor_os_cost
, production_infra_cost
, business_os_cost
FROM
  final_join;


-- hive.bi_silver.hr_master_report source

CREATE VIEW hive.bi_silver.hr_master_report SECURITY DEFINER AS
WITH
  full_hr_raw AS (
   SELECT
     o.*
   , COALESCE(r.termination_date, o.termination_date) termination_date_r
   , r.employment_status
   , r.termination_reason
   , r.region
   FROM
     (hive.bi_silver.hr_employee_onboard o
   LEFT JOIN hive.bi_silver.hr_employee_resigned r ON (o.employee_code = r.employee_code))
) 
, ranked_hr AS (
   SELECT
     *
   , ROW_NUMBER() OVER (PARTITION BY employee_code ORDER BY COALESCE(CAST(termination_date_r AS DATE), DATE '0001-01-01') DESC, COALESCE(CAST(hire_date AS DATE), DATE '0001-01-01') DESC) rn
   FROM
     full_hr_raw
) 
SELECT
  employee_code
, UPPER(email_nametag) email_username
, UPPER(current_status) current_status
, UPPER(division) division
, UPPER(business_unit_level_1) business_unit_level_1
, UPPER(department_level_2) department_level_2
, UPPER(job_level) job_level
, UPPER(management_level) management_level
, CAST(hire_date AS timestamp) hire_date
, CAST(termination_date_r AS timestamp) termination_date
, gender
, CAST(((year(current_date) - year(date_of_birth)) - (CASE WHEN (date_format(current_date, 'MM-dd') < date_format(date_of_birth, 'MM-dd')) THEN 1 ELSE 0 END)) AS INT) age
, UPPER(COALESCE(employment_status, 'ACTIVE')) employment_status
, termination_reason
, UPPER(talent_group) talent_group
, UPPER(performance_level) performance_level
, UPPER(potential_level) potential_level
, (CASE WHEN (is_key_employee = 1) THEN 1 ELSE 0 END) is_key_employee_flag
FROM
  ranked_hr
WHERE (rn = 1);


-- hive.bi_silver.cx_cso_customer_ticket_monthly source

CREATE VIEW hive.bi_silver.cx_cso_customer_ticket_monthly SECURITY DEFINER AS
SELECT
  UPPER(product_category) product_category
, UPPER(company_name) company_name
, DATE_TRUNC('month', created_at) report_month
, COUNT(DISTINCT ticket_id) ticket_count
, MAX(UPPER(customer_group)) customer_group
, MAX(UPPER(customer_segment_l1)) customer_segment_l1
, SUM(agent_reply_count) total_agent_reply_count
, SUM((CASE WHEN is_overdue THEN 1 ELSE 0 END)) overdue_ticket_count
, SUM((CASE WHEN (violated_level > 0) THEN 1 ELSE 0 END)) sla_violated_ticket_count
, AVG((CASE WHEN (resolved_at IS NOT NULL) THEN (date_diff('minute', created_at, resolved_at) / 60) END)) avg_resolve_time_minutes
FROM
  hive.bi_silver.cx_cso_support_tickets
GROUP BY 1, 2, 3;

-- hive.bi_silver.hr_performance_rate

CREATE TABLE hive.bi_silver.hr_performance_rate (
   employee_id varchar,
   employee_name varchar,
   username varchar,
   business_email varchar,
   gender varchar,
   age_group varchar,
   branch_name varchar,
   contract_company_type varchar,
   evaluation_date date,
   hire_date_vcs timestamp(3),
   termination_date timestamp(3),
   job_title varchar,
   tenure varchar,
   n1_group varchar,
   n2_group varchar,
   employee_level varchar,
   score varchar,
   ki varchar,
   crawled_at_ts timestamp(3)
)
WITH (
      format = 'PARQUET'
);

-- hive.bi_silver.crm_activity_history

CREATE TABLE hive.bi_silver.crm_activity_history (
   id varchar,
   activity_name varchar,
   am_user_id varchar,
   contact_id varchar,
   company_id varchar,
   deal_id varchar,
   created_at timestamp(3),
   creator_id varchar,
   updater_id varchar,
   updated_at timestamp(3),
   recent_note varchar,
   record_type_id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bi_silver.crm_business_rules

CREATE TABLE hive.bi_silver.crm_business_rules (
   id varchar,
   business_rule_name varchar,
   am_user_id varchar,
   rule_type varchar,
   price_type varchar,
   parent_product_id varchar,
   child_product_id varchar,
   ratio_operator varchar,
   ratio_value bigint,
   ratio_unit varchar,
   is_active boolean,
   created_at timestamp(3),
   creator_id varchar,
   updated_at timestamp(3),
   updater_id varchar,
   recent_note varchar,
   record_type_id varchar
)
WITH (
   format = 'PARQUET'
);




CREATE OR REPLACE VIEW hive.bi_silver.crm_allocation_details SECURITY DEFINER AS
with allocation_tb as (
SELECT payment_date, deal_id, deal_stage_name, product_category, product_type,deal_type, vnd_amount, currency_amount, currency_code, company_alias,am_username,is_recurring,is_in_year,sign_date,fac_date, probability,am_group
,   true as is_confirmed 
, customer_segment_l1
,        customer_segment_l2
,        CASE
            WHEN customer_segment_l1 = 'Nội bộ'
              OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 = 'Direct / Local Channel'
              )
                THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'

            WHEN customer_segment_l1 = 'International'
             AND customer_segment_l2 = 'Viettel Global Partner'
                THEN 'DT Quốc tế (KH ngoài QT)'

            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'

            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'

            ELSE 'Khác'
        END AS customer_segment
      ,channel_name
      , is_actual
      , am_territory_name

FROM hive.bi_silver.crm_committed_revenue
union all
SELECT  payment_date, deal_id, deal_stage_name, product_category, product_type,deal_type, vnd_amount , currency_amount, currency_code, company_alias,am_username,is_recurring,is_in_year,sign_date,fac_date, probability,am_group
, false as is_confirmed   
, customer_segment_l1
,        customer_segment_l2
,        CASE
            WHEN customer_segment_l1 = 'Nội bộ'
              OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 = 'Direct / Local Channel'
              )
                THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'

            WHEN customer_segment_l1 = 'International'
             AND customer_segment_l2 = 'Viettel Global Partner'
                THEN 'DT Quốc tế (KH ngoài QT)'

            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'

            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'

            ELSE 'Khác'
        END AS customer_segment
      ,channel_name
      , false as is_actual
      , am_territory_name
FROM hive.bi_silver.crm_expected_revenue
)
select tb1.*, tb2.deal_name from allocation_tb tb1
LEFT JOIN hive.bi_silver.crm_deals tb2 on tb1.deal_id = tb2.deal_id