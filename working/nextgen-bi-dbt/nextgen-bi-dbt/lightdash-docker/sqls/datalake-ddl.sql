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
   auto_purge = true,
   external_location = 's3a://bi-silver/contract_collected_invoices',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_committed_revenue definition

CREATE TABLE hive.bi_silver.crm_committed_revenue (
   report_date timestamp(3),
   report_year integer,
   report_month integer,
   product_category_code varchar,
   product_category varchar,
   product_growth_type varchar,
   product_service_group varchar,
   revenue_amount decimal(18, 2),
   usd_to_vnd double,
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
   external_location = 's3a://bi-silver/crm_committed_revenue',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_company_contacts definition

CREATE TABLE hive.bi_silver.crm_company_contacts (
   company_id varchar,
   contact_id varchar,
   company_name varchar,
   created_at timestamp(3),
   contact_name varchar,
   email varchar,
   source_system varchar,
   updated_at timestamp(3)
)
WITH (
   external_location = 's3a://bi-silver/crm_company_contacts',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_contract_allocations definition

CREATE TABLE hive.bi_silver.crm_contract_allocations (
   deal_id varchar,
   deal_name varchar,
   deal_stage_name varchar,
   contract_id varchar,
   usd_to_vnd double,
   vnd_to_usd double,
   contract_type varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   customer_group varchar,
   company_id varchar,
   company_name varchar,
   company_alias varchar,
   currency_code varchar,
   am_username varchar,
   due_date timestamp(3),
   fac_date timestamp(3),
   pricebook_id varchar,
   product_category_index varchar,
   product_description varchar,
   product_category varchar,
   product_type varchar,
   deployment_type varchar,
   vat decimal(18, 2),
   is_recurring boolean,
   product_index varchar,
   territory_name varchar,
   product_total_value decimal(18, 2),
   sales_performance_value decimal(18, 2),
   period_value bigint,
   period_number integer,
   period_name varchar,
   first_payment_date timestamp(3),
   invoice_activation_date timestamp(3),
   created_at timestamp(3),
   updated_at timestamp(3),
   closed_date timestamp(3),
   days_since_last_update integer,
   sale_admin varchar,
   id varchar,
   period_index integer
)
WITH (
   external_location = 's3a://bi-silver/crm_contract_allocations',
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
   external_location = 's3a://bi-silver/crm_contracts',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_deal_interested_products definition

CREATE TABLE hive.bi_silver.crm_deal_interested_products (
   deal_id varchar,
   created_at timestamp(3),
   interested_product_category varchar
)
WITH (
   external_location = 's3a://bi-silver/crm_deal_interested_products',
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
   discount_amount decimal(18, 2),
   discount_type varchar,
   final_total_amount decimal(18, 2),
   base_total_amount decimal(18, 2),
   id varchar
)
WITH (
   external_location = 's3a://bi-silver/crm_deal_quotation_products',
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
   external_location = 's3a://bi-silver/crm_deal_quotations',
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
   external_location = 's3a://bi-silver/crm_deal_reasons',
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
   company_tax_code varchar,
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
   external_location = 's3a://bi-silver/crm_deals',
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
   external_location = 's3a://bi-silver/crm_partners',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_expected_revenue definition

CREATE TABLE hive.bi_silver.crm_expected_revenue (
   payment_date timestamp(3),
   deal_id varchar,
   update_at timestamp(3),
   currency_amount decimal(18, 2),
   vnd_amount decimal(18, 2),
   is_actual boolean,
   due_date timestamp(3),
   vat decimal(10, 2),
   product_type varchar,
   deployment_type varchar,
   product_category varchar,
   pricebook_id varchar,
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
   external_location = 's3a://bi-silver/crm_expected_revenue',
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
   created_at timestamp(3),
   updated_at timestamp(3),
   item_code varchar,
   idd varchar
)
WITH (
   external_location = 's3a://bi-silver/crm_pricebook',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_product_category definition

CREATE TABLE hive.bi_silver.crm_product_category (
   category_code varchar,
   product_category varchar
)
WITH (
   external_location = 's3a://bi-silver/crm_product_category',
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
   external_location = 's3a://bi-silver/crm_product_tree',
   format = 'PARQUET'
);


-- hive.bi_silver.crm_product_tree_item_code definition

CREATE TABLE hive.bi_silver.crm_product_tree_item_code (
   idd varchar,
   item_code varchar
)
WITH (
   external_location = 's3a://bi-silver/crm_product_tree_item_code',
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
   incorporation_date timestamp(3),
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
   last_contacted_via_sales_activity timestamp(3),
   last_contacted_sales_activity_mode varchar,
   last_assigned_at timestamp(3),
   renewal_date timestamp(3),
   business_type varchar,
   industry_type varchar
)
WITH (
   external_location = 's3a://bi-silver/crm_sales_accounts',
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
   external_location = 's3a://bi-silver/crm_users',
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
   day_of_week integer,
   day_type varchar,
   product_category_code varchar,
   product_category varchar,
   product_item_type varchar
)
WITH (
   external_location = 's3a://bi-silver/cx_cso_support_tickets',
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
   external_location = 's3a://bi-silver/cx_cso_ticket_tags',
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
   external_location = 's3a://bi-silver/cx_sur_question_answer_choices',
   format = 'PARQUET'
);


-- hive.bi_silver.cx_sur_question_response definition

CREATE TABLE hive.bi_silver.cx_sur_question_response (
   collected_date timestamp(3),
   survey_id varchar,
   response_id varchar,
   respondent_uuid varchar,
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
   kpi_group varchar
)
WITH (
   external_location = 's3a://bi-silver/cx_sur_question_response',
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
   external_location = 's3a://bi-silver/cx_sur_surveys',
   format = 'PARQUET'
);


-- hive.bi_silver.dim_customer_segment_l1 definition

CREATE TABLE hive.bi_silver.dim_customer_segment_l1 (
   segment_l1 varchar
)
WITH (
   external_location = 's3a://bi-silver/dim_customer_segment_l1',
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
   external_location = 's3a://bi-silver/dim_date',
   format = 'PARQUET'
);


-- hive.bi_silver.dim_product_category definition

CREATE TABLE hive.bi_silver.dim_product_category (
   product_category varchar
)
WITH (
   external_location = 's3a://bi-silver/dim_product_category',
   format = 'PARQUET'
);


-- hive.bi_silver.dim_territory_name definition

CREATE TABLE hive.bi_silver.dim_territory_name (
   territory_name varchar
)
WITH (
   external_location = 's3a://bi-silver/dim_territory_name',
   format = 'PARQUET'
);


-- hive.bi_silver.dim_unit_level_1 definition

CREATE TABLE hive.bi_silver.dim_unit_level_1 (
   unit_level_1 varchar COMMENT 'Organization level N'
)
WITH (
   external_location = 's3a://bi-silver/dim_unit_level_1',
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
   external_location = 's3a://bi-silver/finance_actual_cost',
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
   usd_to_vnd double,
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
   external_location = 's3a://bi-silver/finance_allocated_revenue',
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
   external_location = 's3a://bi-silver/finance_contract_collected_invoices',
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
   external_location = 's3a://bi-silver/finance_cost_plan',
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
   external_location = 's3a://bi-silver/finance_fact_ratios',
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
   external_location = 's3a://bi-silver/finance_product_revenue_plan',
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
   external_location = 's3a://bi-silver/finance_production_cost_allocations',
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
   external_location = 's3a://bi-silver/finance_revenue_plan',
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
   external_location = 's3a://bi-silver/giang_test_masterplan',
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
   external_location = 's3a://bi-silver/hr_employee_headcount',
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
   external_location = 's3a://bi-silver/hr_employee_onboard',
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
   external_location = 's3a://bi-silver/hr_employee_resigned',
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
   external_location = 's3a://bi-silver/jira_msn',
   format = 'PARQUET'
);


-- hive.bi_silver.jira_task_operation definition

CREATE TABLE hive.bi_silver.jira_task_operation (
   key varchar,
   start_date timestamp(3),
   due_date timestamp(3),
   summary varchar,
   issue_type varchar,
   priority varchar,
   status varchar,
   assignor varchar,
   work_group varchar,
   department_center varchar,
   assignee varchar,
   created timestamp(3),
   updated timestamp(3)
)
WITH (
   external_location = 's3a://bi-silver/jira_task_operation',
   format = 'PARQUET'
);


-- hive.bi_silver.noc_entities definition

CREATE TABLE hive.bi_silver.noc_entities (
   customer varchar,
   project varchar,
   host varchar
)
WITH (
   external_location = 's3a://bi-silver/noc_entities',
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
   external_location = 's3a://bi-silver/noc_metrics',
   format = 'PARQUET'
);