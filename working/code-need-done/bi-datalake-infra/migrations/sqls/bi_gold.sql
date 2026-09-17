SHOW SCHEMAS FROM hive;
CREATE SCHEMA hive.bi_gold 
with (location='s3a://bi-gold/');


-- hive.bi_gold.crm_contract_allocations source

CREATE VIEW hive.bi_gold.crm_contract_allocations SECURITY DEFINER AS
SELECT
  CAST(first_payment_date AS timestamp) first_payment_date
, CAST(invoice_activation_date AS timestamp) invoice_activation_date
, CAST(fac_date AS timestamp) fac_date
, CAST(due_date AS timestamp) due_date
, upper(product_category) product_category
, upper(product_type) product_type
, upper(deployment_type) deployment_type
, upper(customer_group) customer_group
, upper(territory_name) territory_name
, upper(territory_name) market_name
, upper(customer_segment_l1) customer_segment_l1
, upper(customer_segment_l2) customer_segment_l2
, upper(customer_segment_l3) customer_segment_l3
, upper(currency_code) currency_code
, is_recurring
, period_number
, upper(period_name) period_name
, period_value
, product_total_value
, sales_performance_value
, contract_id
FROM
  bi_silver.crm_contract_allocations;


-- hive.bi_gold.crm_contracts source

CREATE VIEW hive.bi_gold.crm_contracts SECURITY DEFINER AS
SELECT
  CAST(sign_date AS timestamp) sign_date
, CAST(fac_date AS timestamp) fac_date
, CAST(expire_date AS timestamp) expire_date
, upper(contract_type) contract_type
, upper(signing_method) signing_method
, upper(customer_segment_l1) customer_segment_l1
, upper(customer_segment_l2) customer_segment_l2
, upper(customer_segment_l3) customer_segment_l3
, upper(status) contract_status
, upper(customer_group) customer_group
, upper(currency_code) currency_code
, contract_amount
, vcs_contract_amount
, viettel_contract_amount
, deal_id
, id
FROM
  bi_silver.crm_contracts;


-- hive.bi_gold.crm_deal_interested_product_stats source

CREATE VIEW hive.bi_gold.crm_deal_interested_product_stats SECURITY DEFINER AS
SELECT
  UPPER(interested_product_category) product_category
, count(*) number_of_deals
FROM
  bi_silver.crm_deal_interested_products
GROUP BY 1;


-- hive.bi_gold.crm_deal_quotation_products source

CREATE VIEW hive.bi_gold.crm_deal_quotation_products SECURITY DEFINER AS
SELECT
  CAST(quotation_created_at AS timestamp) quotation_date
, upper(quotation_status) quotation_status
, upper(product_name) product_name
, global_discount_type
, global_discount global_discount_amount
, upper(package) package
, base_price
, quantitative
, unit product_unit
, upper(discount_type) discount_type
, discount discount_amount
, final_total_amount
, base_total_amount
, deal_id
FROM
  bi_silver.crm_deal_quotation_products;


-- hive.bi_gold.crm_deal_reason_stats source

CREATE VIEW hive.bi_gold.crm_deal_reason_stats SECURITY DEFINER AS
SELECT
  UPPER(reason_name) reason_name
, count(*) number_of_deals
FROM
  bi_silver.crm_deal_reasons
GROUP BY 1;


-- hive.bi_gold.crm_deals source

CREATE VIEW hive.bi_gold.crm_deals SECURITY DEFINER AS
SELECT
  CAST(created_at AS timestamp) deal_date
, CAST(due_date AS timestamp) due_date
, CAST(sign_date AS timestamp) sign_date
, CAST(last_assigned_at AS timestamp) last_assigned_date
, CAST(fac_date AS timestamp) fac_date
, CAST(stage_updated_time AS timestamp) stage_updated_date
, CAST(closed_date AS timestamp) closed_date
, CAST(expected_close_date AS timestamp) expected_close_date
, UPPER(customer_group) customer_group
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l2) customer_segment_l2
, UPPER(customer_segment_l3) customer_segment_l3
, UPPER(channel) deal_channel
, UPPER(territory_name) territory_name
, UPPER(territory_name) market_name
, UPPER(deal_status) deal_status
, UPPER(deal_stage_name) deal_stage_name
, UPPER(probability_bucket) probability_bucket
, UPPER(company_province) company_province
, UPPER(currency_code) currency_code
, expected_deal_value
, budget_amount
, currency_amount
, vnd_amount
, probability
, deal_id
, contract_id
, UPPER(company_name) customer_name
, UPPER(contract_type) contract_type
FROM
  bi_silver.crm_deals;


-- hive.bi_gold.crm_partners source

CREATE VIEW hive.bi_gold.crm_partners SECURITY DEFINER AS
SELECT
  id partner_id
, UPPER(COALESCE(country, 'Unknown')) country
, UPPER(COALESCE(partner_type, 'Unknown')) partner_type
, UPPER(COALESCE(partner_status, 'Unknown')) partner_status
, UPPER(COALESCE(document_type, 'Unknown')) document_type
, effective_date
, expiration_date
FROM
  bi_silver.crm_partners;


-- hive.bi_gold.crm_payment_forecast source

CREATE VIEW hive.bi_gold.crm_payment_forecast COMMENT 'Forecast revenue dataset based on forecast invoices. Used for revenue forecast and cash collection planning.' SECURITY DEFINER AS
SELECT
  CAST(payment_date AS timestamp) payment_date
, CAST(due_date AS timestamp) due_date
, UPPER(product_type) product_type
, UPPER(deployment_type) deployment_type
, UPPER(product_category) product_category
, UPPER(territory_name) territory_name
, UPPER(territory_name) market_name
, UPPER(COALESCE(customer_group, 'Unknown')) customer_group
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l2) customer_segment_l2
, UPPER(COALESCE(customer_segment_l3, 'Unknown')) customer_segment_l3
, UPPER(currency_code) currency_code
, is_recurring
, is_actual
, vnd_amount
, currency_amount
, contract_id
, deal_id
FROM
  bi_silver.crm_payment_forecast;

-- hive.bi_gold.crm_allocated_revenue source

CREATE VIEW hive.bi_gold.crm_allocated_revenue SECURITY DEFINER AS
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(product_category_code) product_category_code
, UPPER(product_category) product_category
, UPPER(product_service_group) product_service_group
, UPPER(revenue_type) revenue_type
, UPPER(channel_name) channel_name
, UPPER(department_alias) sales_department
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l3) customer_segment_l3
, UPPER(revenue_group) revenue_group
, UPPER(payment_stage) payment_stage
, UPPER(business_unit_level_1) business_unit_level_1
, UPPER(territory_name) territory_name
, revenue_amount
FROM
  bi_silver.crm_allocated_revenue;


-- hive.bi_gold.crm_pricebook source

CREATE VIEW hive.bi_gold.crm_pricebook SECURITY DEFINER AS
SELECT
  CAST(created_at AS timestamp) created_at
, CAST(updated_at AS timestamp) updated_at
, UPPER(product_category) product_category
, UPPER(product_version) product_version
, UPPER(item_type) item_type
, UPPER(product_type) product_type
, UPPER(sub_type) sub_type
, UPPER(product_license) product_license
, UPPER(price_type) price_type
, UPPER(package) package
, UPPER(currency_code) currency_code
, is_quantity_based
, is_related_csmp
, csmp_discount
, csmp_discount_silver
, csmp_discount_gold
, csmp_discount_diamond
, price_unit
, price
, price_min
, price_max
FROM
  bi_silver.crm_pricebook;


-- hive.bi_gold.crm_sales_accounts source

CREATE VIEW hive.bi_gold.crm_sales_accounts SECURITY DEFINER AS
SELECT
  id
, UPPER(country) country
, UPPER(state) state
, UPPER(city) city
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l2) customer_segment_l2
, UPPER(COALESCE(customer_segment_l3, 'Unknown')) customer_segment_l3
, CAST(created_at AS timestamp) created_at
, UPPER(business_type) business_type
, UPPER(industry_type) industry_type
, using_soc
FROM
  bi_silver.crm_sales_accounts;


-- hive.bi_gold.cx_cso_support_tickets source

CREATE VIEW hive.bi_gold.cx_cso_support_tickets SECURITY DEFINER AS
SELECT
  UPPER(action_program) action_program
, UPPER(assigned_to) assigned_to
, UPPER(call_reminder) call_reminder
, UPPER(issue_category) issue_category
, closed_at
, UPPER(communication_effectiveness) communication_effectiveness
, UPPER(company_alias) company_alias
, UPPER(company_name) company_name
, created_at
, UPPER(customer_entry_channel) customer_entry_channel
, customer_respond_date
, customer_satisfaction_rating
, UPPER(customer_group) customer_group
, first_responded_at
, first_response_time_minutes
, imported_ticket_date
, imported_ticket_due_date
, incident_root_cause
, is_duplicated_ticket
, is_overdue
, is_vcs
, UPPER(issues_type) issues_type
, l1
, l1_time_actual_minutes
, l1_time_allowed_minutes
, l1_violated
, l2
, l2_time_actual_minutes
, l2_time_allowed_minutes
, l2_violated
, l3
, l3_time_actual_minutes
, l3_time_allowed_minutes
, l3_violated
, l4
, l4_time_actual_minutes
, l4_time_allowed_minutes
, l4_violated
, UPPER(note) note
, is_notification
, number_of_due_date_changes
, old_due_date
, is_one_day_before_due
, is_one_hour_before_due
, UPPER(priority_level) priority_level
, UPPER(urgency_level) urgency_level
, UPPER(reason) reason
, related_ticket
, is_reminder_update
, UPPER(requester_name) requester_name
, resolution_time_minutes
, resolved_at
, response_time_minutes
, UPPER(sentiment) sentiment
, UPPER(severity_level) severity_level
, UPPER(spam_type) spam_type
, UPPER(status_name) status_name
, UPPER(subject) subject
, UPPER(support_category) support_category
, is_third_four_time
, is_reopened_by_cx
, ticket_id
, time_to_response_minutes
, ttr_overdue
, updated_at
, assigned_agent_stage
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l2) customer_segment_l2
, UPPER(customer_segment_l3) customer_segment_l3
, violated_level
FROM
  bi_silver.cx_cso_support_tickets;


-- hive.bi_gold.cx_cso_ticket_tags source

CREATE VIEW hive.bi_gold.cx_cso_ticket_tags SECURITY DEFINER AS
SELECT
  created_at
, UPPER(tag) tag
, ticket_id
FROM
  bi_silver.cx_cso_ticket_tags;


-- hive.bi_gold.cx_sur_question_answer_choices source

CREATE VIEW hive.bi_gold.cx_sur_question_answer_choices SECURITY DEFINER AS
SELECT
  UPPER(product_category) product_category
, UPPER(question_type) question_type
, UPPER(journey) journey
, UPPER(kpi_type) kpi_type
, UPPER(customer_touchpoint) customer_touchpoint
, UPPER(question_clean) question_clean
, survey_id
, question_id
, answer_choice_id
FROM
  bi_silver.cx_sur_question_answer_choices;


-- hive.bi_gold.cx_sur_question_response source

CREATE VIEW hive.bi_gold.cx_sur_question_response SECURITY DEFINER AS
SELECT
  collected_date
, UPPER(product_category) product_category
, UPPER(question_type) question_type
, UPPER(answer_string) answer_string
, UPPER(answer_tag) answer_tag
, item_score_value
, UPPER(item_score_label) item_score_label
, UPPER(kpi_type) kpi_type
, UPPER(journey) journey
, UPPER(customer_touchpoint) customer_touchpoint
, UPPER(kpi_group) kpi_group
, UPPER(choice_content) choice_content
, UPPER(question_clean) question_clean
, survey_id
, response_id
, respondent_uuid
, question_id
, choice_id
, answer_number
FROM
  bi_silver.cx_sur_question_response;


-- hive.bi_gold.cx_sur_surveys source

CREATE VIEW hive.bi_gold.cx_sur_surveys SECURITY DEFINER AS
SELECT
  created_at
, UPPER(survey_type) survey_type
, UPPER(folder_name) folder_name
, UPPER(journey) journey
, UPPER(customer_touchpoint) customer_touchpoint
, UPPER(product_category) product_category
, survey_id
FROM
  bi_silver.cx_sur_surveys;


-- hive.bi_gold.dim_customer_segment_l1 source

CREATE VIEW hive.bi_gold.dim_customer_segment_l1 SECURITY DEFINER AS
SELECT *
FROM
  bi_silver.dim_customer_segment_l1;


-- hive.bi_gold.dim_date source

CREATE VIEW hive.bi_gold.dim_date SECURITY DEFINER AS
SELECT *
FROM
  bi_silver.dim_date;


-- hive.bi_gold.dim_product_category source

CREATE VIEW hive.bi_gold.dim_product_category SECURITY DEFINER AS
SELECT *
FROM
  bi_silver.dim_product_category;


-- hive.bi_gold.dim_territory_name source

CREATE VIEW hive.bi_gold.dim_territory_name SECURITY DEFINER AS
SELECT *
FROM
  bi_silver.dim_territory_name;


-- hive.bi_gold.dim_unit_level_1 source

CREATE VIEW hive.bi_gold.dim_unit_level_1 SECURITY DEFINER AS
SELECT *
FROM
  bi_silver.dim_unit_level_1;


-- hive.bi_gold.finance_actual_cost source

CREATE VIEW hive.bi_gold.finance_actual_cost SECURITY DEFINER AS
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(old_category) old_category
, UPPER(product_category_code) product_category_code
, UPPER(cost_group) cost_group
, UPPER(territory_name) territory_name
, UPPER(product_category) product_category
, UPPER(business_unit_level_1) business_unit_level_1
, base_currency_amount
FROM
  bi_silver.finance_actual_cost;


-- hive.bi_gold.finance_allocated_revenue source

CREATE VIEW hive.bi_gold.finance_allocated_revenue SECURITY DEFINER AS
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(product_category_code) product_category_code
, UPPER(product_category) product_category
, UPPER(product_service_group) product_service_group
, UPPER(revenue_type) revenue_type
, UPPER(channel_name) channel_name
, UPPER(department_alias) sales_department
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l3) customer_segment_l3
, UPPER(revenue_group) revenue_group
, UPPER(payment_stage) payment_stage
, UPPER(business_unit_level_1) business_unit_level_1
, UPPER(territory_name) territory_name
, revenue_amount
FROM
  bi_silver.finance_allocated_revenue;


-- hive.bi_gold.finance_contract_collected_invoices source

CREATE VIEW hive.bi_gold.finance_contract_collected_invoices SECURITY DEFINER AS
SELECT
  idd
, upper(sales_channel) sales_channel
, upper(transaction_code) transaction_code
, upper(invoice_number) invoice_number
, upper(contract_num) contract_num
, upper(customer_code) customer_code
, upper(contract_description) contract_description
, upper(company_name) company_name
, original_amount
, initial_receivable_amount
, total_received_amount
, remaining_balance_amount
, invoice_date
, payment_deadline
, payment_date
, overdue_months_numeric
, upper(overdue_status_enum) overdue_status_enum
, upper(am_username) am_username
FROM
  bi_silver.finance_contract_collected_invoices;


-- hive.bi_gold.finance_cost_plan source

CREATE VIEW hive.bi_gold.finance_cost_plan SECURITY DEFINER AS
SELECT
  CAST(plan_date AS timestamp) plan_date
, UPPER(cost_group) cost_group
, expected_cost_value
FROM
  bi_silver.finance_cost_plan;


-- hive.bi_gold.finance_daily_business_metrics source

CREATE VIEW hive.bi_gold.finance_daily_business_metrics SECURITY DEFINER AS
WITH
  actual_cost AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , business_unit_level_1 business_unit_level_1
   , territory_name
   , SUM(base_currency_amount) actual_cost_amount
   FROM
     bi_silver.finance_actual_cost
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
     bi_silver.crm_allocated_revenue
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
     bi_silver.crm_allocated_revenue
   GROUP BY CAST(report_date AS DATE), product_category, territory_name
) 
, revenue_plan AS (
   SELECT
     CAST(plan_date AS DATE) report_date
   , product_group product_category
   , null business_unit_level_1
   , null territory_name
   , SUM(plan_must_amount) planned_revenue_must_amount
   , SUM(plan_nice_amount) planned_revenue_nice_amount
   FROM
     bi_silver.finance_product_revenue_plan
   GROUP BY CAST(plan_date AS DATE), product_group
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


-- hive.bi_gold.finance_metrics source

CREATE VIEW hive.bi_gold.finance_metrics SECURITY DEFINER AS
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
     bi_silver.crm_allocated_revenue
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
     bi_silver.finance_actual_cost
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


-- hive.bi_gold.finance_product_revenue_plan source

CREATE VIEW hive.bi_gold.finance_product_revenue_plan SECURITY DEFINER AS
SELECT
  CAST(plan_date AS timestamp) plan_date
, UPPER(product_group) product_group
, UPPER(product_name) product_name
, plan_must_amount
, plan_nice_amount
FROM
  bi_silver.finance_product_revenue_plan;


-- hive.bi_gold.finance_production_cost_allocations source

CREATE VIEW hive.bi_gold.finance_production_cost_allocations SECURITY DEFINER AS
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(old_category) old_product_category
, UPPER(cost_group) cost_group
, UPPER(territory_name) territory_name
, UPPER(market_name) market_name
, base_currency_amount
FROM
  bi_silver.finance_production_cost_allocations;


-- hive.bi_gold.finance_revenue_plan source

CREATE VIEW hive.bi_gold.finance_revenue_plan SECURITY DEFINER AS
SELECT
  CAST(plan_date AS timestamp) plan_date
, UPPER(customer_segment_l1) customer_segment_l1
, UPPER(customer_segment_l2) customer_segment_l2
, UPPER(segment_l1_alias) customer_segment_alias
, plan_viettel_group_amount
, plan_must_amount
, plan_nice_amount
FROM
  bi_silver.finance_revenue_plan;


-- hive.bi_gold.hr_master_report source

CREATE VIEW hive.bi_gold.hr_master_report SECURITY DEFINER AS
WITH
  full_hr_raw AS (
   SELECT
     o.*
   , COALESCE(r.termination_date, o.termination_date) termination_date_r
   , r.employment_status
   , r.termination_reason
   , r.region
   FROM
     (bi_silver.hr_employee_onboard o
   LEFT JOIN bi_silver.hr_employee_resigned r ON (o.employee_code = r.employee_code))
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