-- hive.bi_silver_dev.contract_collected_invoices source

CREATE OR REPLACE VIEW hive.bi_silver_dev.contract_collected_invoices SECURITY DEFINER AS
SELECT
  idd
, sales_channel
, transaction_code
, (CASE WHEN (invoice_number IS NOT NULL) THEN regexp_replace(invoice_number, '^(.{3}).*(.{2})$', '$1****$2') ELSE null END) invoice_number
, (CASE WHEN (contract_num IS NOT NULL) THEN regexp_replace(contract_num, '^(.{3}).*(.{2})$', '$1****$2') ELSE null END) contract_num
, (CASE WHEN (customer_code IS NOT NULL) THEN regexp_replace(customer_code, '^(.{2}).*(.{2})$', '$1****$2') ELSE null END) customer_code
, contract_description
, (CASE WHEN (company_name IS NOT NULL) THEN concat(substr(company_name, 1, 3), '***') ELSE null END) company_name
, original_currency
, (CASE WHEN (initial_receivable_amount IS NOT NULL) THEN CAST((TRY_CAST(initial_receivable_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) initial_receivable_amount
, (CASE WHEN (total_received_amount IS NOT NULL) THEN CAST((TRY_CAST(total_received_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) total_received_amount
, (CASE WHEN (remaining_balance_amount IS NOT NULL) THEN CAST((TRY_CAST(remaining_balance_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) remaining_balance_amount
, invoice_date
, payment_deadline
, payment_date
, overdue_months_numeric
, overdue_status_enum
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 2), '***') ELSE null END) am_username
FROM
  hive.bi_silver.contract_collected_invoices;


-- hive.bi_silver_dev.crm_committed_revenue source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_committed_revenue SECURITY DEFINER AS
SELECT
  report_date
, report_year
, report_month
, product_category_code
, product_category
, product_growth_type
, product_service_group
, (CASE WHEN (revenue_amount IS NOT NULL) THEN CAST((TRY_CAST(revenue_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) revenue_amount
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 5), '***') ELSE null END) company_alias
, revenue_type
, channel_name
, is_soc
, department_alias
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, revenue_group
, (CASE WHEN (vat IS NOT NULL) THEN CAST((TRY_CAST(vat AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vat
, payment_stage
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 5), '***') ELSE null END) am_username
, (CASE WHEN (presale_name IS NOT NULL) THEN concat(substr(presale_name, 1, 5), '***') ELSE null END) presale_name
, currency_code
, business_unit_level_1
, territory_name
, deal_id
, update_at
, (CASE WHEN (currency_amount IS NOT NULL) THEN CAST((TRY_CAST(currency_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) currency_amount
, is_actual
, due_date
, deployment_type
, pricebook_id
, is_recurring
, product_index
, customer_id
, (CASE WHEN (customer_name IS NOT NULL) THEN concat(substr(customer_name, 1, 5), '***') ELSE null END) customer_name
, customer_group
, product_category_index
, contract_id
, payment_index
, contract_allocation_id
, usd_to_vnd
, am_group
, is_in_year
, payment_period_start_date
, payment_period_end_date
, product_total_value
, payment_date
, sign_date
, fac_date
, contract_expire_date
, product_type
, probability
, contract_number
, contract_total_days
, sales_admin_username
, team_name
, deal_stage_name
FROM
  hive.bi_silver.crm_committed_revenue;


-- hive.bi_silver_dev.crm_contract_allocations source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_contract_allocations SECURITY DEFINER AS
SELECT
  deal_id
, (CASE WHEN (deal_name IS NOT NULL) THEN concat(substr(deal_name, 6, 12), '***') ELSE null END) deal_name
, deal_stage_name
, (CASE WHEN (contract_id IS NOT NULL) THEN regexp_replace(contract_id, '^(.{3}).*(.{2})$', '$1****$2') ELSE null END) contract_id
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, customer_group
, company_id
, (CASE WHEN (company_name IS NOT NULL) THEN concat(substr(company_name, 1, 3), '***') ELSE null END) company_name
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 2), '***') ELSE null END) company_alias
, currency_code
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 2), '***') ELSE null END) am_username
, due_date
, fac_date
, pricebook_id
, product_category_index
, product_description
, product_category
, product_type
, deployment_type
, is_recurring
, product_index
, territory_name
, (CASE WHEN (product_total_value IS NOT NULL) THEN CAST((TRY_CAST(product_total_value AS DOUBLE) / 1000) AS BIGINT) ELSE null END) product_total_value
, (CASE WHEN (sales_performance_value IS NOT NULL) THEN CAST((TRY_CAST(sales_performance_value AS DOUBLE) / 1000) AS BIGINT) ELSE null END) sales_performance_value
, (CASE WHEN (period_value IS NOT NULL) THEN CAST((TRY_CAST(period_value AS DOUBLE) / 1000) AS BIGINT) ELSE null END) period_value
, period_number
, period_name
, invoice_activation_date
, created_at
, updated_at
, closed_date
, days_since_last_update
, (CASE WHEN (sale_admin IS NOT NULL) THEN concat(substr(sale_admin, 1, 2), '***') ELSE null END) sale_admin
, first_payment_date
, id
, period_index
FROM
  hive.bi_silver.crm_contract_allocations;


-- hive.bi_silver_dev.crm_contracts source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_contracts SECURITY DEFINER AS
SELECT
  id
, (CASE WHEN (name IS NOT NULL) THEN concat(substr(name, 1, 5), '***') ELSE null END) name
, (CASE WHEN (user_am_id IS NOT NULL) THEN concat(substr(user_am_id, 1, 2), '***') ELSE null END) user_am_id
, deal_id
, (CASE WHEN (contract_number IS NOT NULL) THEN regexp_replace(contract_number, '^(.{3}).*(.{2})$', '$1****$2') ELSE null END) contract_number
, contract_type
, signing_method
, signing_method_code
, sale_account_id
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 2), '***') ELSE null END) company_alias
, (CASE WHEN (customer_name IS NOT NULL) THEN concat(substr(customer_name, 1, 3), '***') ELSE null END) customer_name
, (CASE WHEN (tax_code IS NOT NULL) THEN to_hex(md5(to_utf8(tax_code))) ELSE null END) tax_code
, customer_group_vi
, segment_l1
, customer_segment_l1
, segment_l2
, customer_segment_l2
, segment_l3
, customer_segment_l3
, status
, status_code
, status_vi
, customer_group
, is_vip_customer
, is_enterprise_customer
, is_state_owned
, is_private_enterprise
, is_banking_group
, is_international_client
, is_internal_client
, sign_date
, fac_date
, duration
, expire_date
, usd_exchange_rate
, currency_code
, (CASE WHEN (contract_amount IS NOT NULL) THEN CAST((TRY_CAST(contract_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) contract_amount
, (CASE WHEN (vat IS NOT NULL) THEN CAST((TRY_CAST(vat AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vat
, (CASE WHEN (vcs_contract_amount IS NOT NULL) THEN CAST((TRY_CAST(vcs_contract_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vcs_contract_amount
, (CASE WHEN (viettel_contract_amount IS NOT NULL) THEN CAST((TRY_CAST(viettel_contract_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) viettel_contract_amount
, partner_id
FROM
  hive.bi_silver.crm_contracts;


-- hive.bi_silver_dev.crm_deal_interested_product_stats source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_interested_product_stats SECURITY DEFINER AS
SELECT
  interested_product_category product_category
, count(*) number_of_deals
FROM
  hive.bi_silver.crm_deal_interested_products
GROUP BY 1;


-- hive.bi_silver_dev.crm_deal_interested_products source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_interested_products SECURITY DEFINER AS
SELECT
  deal_id
, created_at
, interested_product_category
FROM
  hive.bi_silver.crm_deal_interested_products;


-- hive.bi_silver_dev.crm_deal_quotation_products source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_quotation_products SECURITY DEFINER AS
SELECT
  deal_id
, quotation_id
, product_line_no
, (CASE WHEN (product_id IS NOT NULL) THEN regexp_replace(product_id, '^(.{2}).*(.{2})$', '$1****$2') ELSE null END) product_id
, quotation_created_at
, quotation_status
, quotation_lang
, currency_code
, global_discount
, global_discount_type
, product_name
, base_price
, quantitative
, unit
, product_unit
, duration
, package
, (CASE WHEN (vat IS NOT NULL) THEN CAST((TRY_CAST(vat AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vat
, (CASE WHEN (discount_amount IS NOT NULL) THEN CAST((TRY_CAST(discount_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) discount_amount
, discount_type
, (CASE WHEN (final_total_amount IS NOT NULL) THEN CAST((TRY_CAST(final_total_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) final_total_amount
, (CASE WHEN (base_total_amount IS NOT NULL) THEN CAST((TRY_CAST(base_total_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) base_total_amount
, id
FROM
  hive.bi_silver.crm_deal_quotation_products;


-- hive.bi_silver_dev.crm_deal_quotations source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_quotations SECURITY DEFINER AS
SELECT
  deal_id
, quotation_id
, created_at
, status
, tlang
, currency_code
, global_discount
, global_discount_type
FROM
  hive.bi_silver.crm_deal_quotations;


-- hive.bi_silver_dev.crm_deal_reasons source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_reasons SECURITY DEFINER AS
SELECT
  id
, reason_name
, position
, is_partial
FROM
  hive.bi_silver.crm_deal_reasons;


-- hive.bi_silver_dev.crm_deals source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deals SECURITY DEFINER AS
SELECT
  deal_id
, (CASE WHEN (deal_name IS NOT NULL) THEN concat(substr(deal_name, 1, 5), '***') ELSE null END) deal_name
, (CASE WHEN (currency_amount IS NOT NULL) THEN CAST((TRY_CAST(currency_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) currency_amount
, (CASE WHEN (vnd_amount IS NOT NULL) THEN CAST((TRY_CAST(vnd_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vnd_amount
, expected_close_date
, closed_date
, stage_updated_time
, days_to_close
, days_since_last_update
, is_deal_locked
, usd_to_vnd
, get_live_date
, periodicity
, is_select_viettel
, has_budget
, company_id
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 5), '***') ELSE null END) company_alias
, (CASE WHEN (budget_amount IS NOT NULL) THEN CAST((TRY_CAST(budget_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) budget_amount
, customer_group
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, channel
, opportunity_weight_pct
, bidding_required
, (CASE WHEN (presales_name IS NOT NULL) THEN concat(substr(presales_name, 1, 5), '***') ELSE null END) presales_name
, (CASE WHEN (project_manager IS NOT NULL) THEN concat(substr(project_manager, 1, 5), '***') ELSE null END) project_manager
, (CASE WHEN (sales_admin IS NOT NULL) THEN concat(substr(sales_admin, 1, 5), '***') ELSE null END) sales_admin
, partner_id
, contract_id
, initial_source
, warm_up_source
, territory_name
, contract_duration_month
, is_created_contract
, expire_date
, (CASE WHEN (company_name IS NOT NULL) THEN concat(substr(company_name, 1, 5), '***') ELSE null END) company_name
, (CASE WHEN (company_address IS NOT NULL) THEN concat(substr(company_address, 1, 10), '***') ELSE null END) company_address
, company_province
, company_country
, (CASE WHEN (company_email IS NOT NULL) THEN regexp_replace(company_email, '^(.{2}).+(@.+)$', '$1***$2') ELSE null END) company_email
, (CASE WHEN (company_phone IS NOT NULL) THEN regexp_replace(company_phone, '(\\d{3})\\d{4}(\\d+)', '$1****$2') ELSE null END) company_phone
, (CASE WHEN (company_contact IS NOT NULL) THEN concat(substr(company_contact, 1, 3), '***') ELSE null END) company_contact
, currency_code
, quotation_status
, fac_date
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 5), '***') ELSE null END) am_username
, check_change
, probability
, updated_at
, created_at
, deal_stage_id
, deal_status
, deal_payment_status_id
, age
, (CASE WHEN (recent_note IS NOT NULL) THEN '***MASKED***' ELSE null END) recent_note
, next_scheduled_activity_time
, last_assigned_at
, last_contacted_activity_status
, last_contacted_activity_time
, (CASE WHEN (expected_deal_value IS NOT NULL) THEN CAST((TRY_CAST(expected_deal_value AS DOUBLE) / 1000) AS BIGINT) ELSE null END) expected_deal_value
, signing_delay_days
, am_user_id
, sales_account_id
, deal_type_id
, deal_reason_id
, currency_id
, vnd_to_usd
, deal_payment_status
, deal_stage_name
, sign_date
, deal_stage_code
, due_date
, is_signed
, probability_bucket
, contract_type
, deal_reason
, am_group
FROM
  hive.bi_silver.crm_deals;


-- hive.bi_silver_dev.crm_partners source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_partners SECURITY DEFINER AS
SELECT
  id
, (CASE WHEN (partner_name IS NOT NULL) THEN concat(substr(partner_name, 1, 3), '***') ELSE null END) partner_name
, document_number
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 5), '***') ELSE null END) company_alias
, (CASE WHEN (partner_address IS NOT NULL) THEN concat(substr(partner_address, 1, 10), '***') ELSE null END) partner_address
, country
, (CASE WHEN (contact_name IS NOT NULL) THEN concat(substr(contact_name, 1, 5), '***') ELSE null END) contact_name
, contact_position
, (CASE WHEN (contact_mobile IS NOT NULL) THEN regexp_replace(contact_mobile, '(\\d{3})\\d{4}(\\d+)', '$1****$2') ELSE null END) contact_mobile
, (CASE WHEN (contact_email IS NOT NULL) THEN regexp_replace(contact_email, '^(.{2}).+(@.+)$', '$1***$2') ELSE null END) contact_email
, effective_date
, expiration_date
, partner_type
, document_type
, partner_status
, partner_type_vi
, document_format_vi
, partner_status_vi
FROM
  hive.bi_silver.crm_partners;


-- hive.bi_silver_dev.crm_expected_revenue source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_expected_revenue SECURITY DEFINER AS
SELECT
  payment_date
, deal_id
, update_at
, (CASE WHEN (currency_amount IS NOT NULL) THEN CAST((TRY_CAST(currency_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) currency_amount
, (CASE WHEN (vnd_amount IS NOT NULL) THEN CAST((TRY_CAST(vnd_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vnd_amount
, due_date
, product_type
, deployment_type
, product_category
, is_recurring
, territory_name
, product_index
, customer_id
, (CASE WHEN (customer_name IS NOT NULL) THEN concat(substr(customer_name, 1, 5), '***') ELSE null END) customer_name
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 2), '***') ELSE null END) company_alias
, customer_group
, product_category_index
, currency_code
, deal_stage_name
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 2), '***') ELSE null END) am_username
, contract_id
, payment_index
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, (CASE WHEN (sales_admin_username IS NOT NULL) THEN concat(substr(sales_admin_username, 1, 2), '***') ELSE null END) sales_admin_username
, contract_allocation_id
, payment_period_start_date
, payment_period_end_date
, is_in_year
, sign_date
, contract_expire_date
, fac_date
, product_version
, expected_close_date
, team_name
, am_group
FROM
  hive.bi_silver.crm_expected_revenue;


-- hive.bi_silver_dev.crm_pricebook source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_pricebook SECURITY DEFINER AS
SELECT
  product_id
, product_name
, product_description
, product_group
, product_sub_group
, category_code
, product_category
, sku
, product_version
, item_type
, is_active
, product_type
, sub_type
, product_license
, price_type
, package
, price
, currency_code
, price_min
, price_max
, price_unit
, is_quantity_based
, is_related_csmp
, csmp_discount
, csmp_discount_silver
, csmp_discount_gold
, csmp_discount_diamond
, created_at
, updated_at
, item_code
, idd
FROM
  hive.bi_silver.crm_pricebook;


-- hive.bi_silver_dev.crm_product_category source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_product_category SECURITY DEFINER AS
SELECT
  category_code
, product_category
FROM
  hive.bi_silver.crm_product_category;


-- hive.bi_silver_dev.crm_product_tree source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_product_tree SECURITY DEFINER AS
SELECT
  product_group
, product_sub_group
, category_code
, product_category
, sku
, version
, item_type
, license
, price_type
, unit
, item_code
, idd
FROM
  hive.bi_silver.crm_product_tree;


-- hive.bi_silver_dev.crm_product_tree_item_code source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_product_tree_item_code SECURITY DEFINER AS
SELECT
  idd
, item_code
FROM
  hive.bi_silver.crm_product_tree_item_code;


-- hive.bi_silver_dev.crm_sales_accounts source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_sales_accounts SECURITY DEFINER AS
SELECT
  id
, is_partner
, (CASE WHEN (name IS NOT NULL) THEN concat(substr(name, 1, 3), '***') ELSE null END) name
, (CASE WHEN (address IS NOT NULL) THEN concat(substr(address, 1, 10), '***') ELSE null END) address
, city
, state
, (CASE WHEN (zipcode IS NOT NULL) THEN regexp_replace(zipcode, '^(.{2}).*(.{1})$', '$1***$2') ELSE null END) zipcode
, country
, number_of_employees
, (CASE WHEN (annual_revenue IS NOT NULL) THEN CAST((TRY_CAST(annual_revenue AS DOUBLE) / 1000) AS BIGINT) ELSE null END) annual_revenue
, (CASE WHEN (website IS NOT NULL) THEN regexp_replace(website, '^(https?://)?([^/]+).*$', '$1***') ELSE null END) website
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 5), '***') ELSE null END) am_username
, (CASE WHEN (sales_admin IS NOT NULL) THEN concat(substr(sales_admin, 1, 5), '***') ELSE null END) sales_admin
, (CASE WHEN (phone IS NOT NULL) THEN regexp_replace(phone, '(\\d{3})\\d{4}(\\d+)', '$1****$2') ELSE null END) phone
, (CASE WHEN (open_deals_amount IS NOT NULL) THEN CAST((TRY_CAST(open_deals_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) open_deals_amount
, open_deals_count
, (CASE WHEN (won_deals_amount IS NOT NULL) THEN CAST((TRY_CAST(won_deals_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) won_deals_amount
, won_deals_count
, (CASE WHEN (tax_code IS NOT NULL) THEN to_hex(md5(to_utf8(tax_code))) ELSE null END) tax_code
, company_id
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 5), '***') ELSE null END) company_alias
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, incorporation_date
, initial_source
, warm_up_source
, using_soc
, vcs_soc_others
, soc_brand
, service_level
, created_at
, updated_at
, days_since_update
, parent_sales_account_id
, (CASE WHEN (recent_note IS NOT NULL) THEN '***MASKED***' ELSE null END) recent_note
, last_contacted_via_sales_activity
, last_contacted_sales_activity_mode
, last_assigned_at
, renewal_date
, business_type
, industry_type
FROM
  hive.bi_silver.crm_sales_accounts;


-- hive.bi_silver_dev.crm_users source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_users SECURITY DEFINER AS
SELECT
  id
, (CASE WHEN (email IS NOT NULL) THEN regexp_replace(email, '^(.{2}).+(@.+)$', '$1***$2') ELSE null END) email
, (CASE WHEN (display_name IS NOT NULL) THEN concat(substr(display_name, 1, 5), '***') ELSE null END) display_name
, is_active
, job_title
FROM
  hive.bi_silver.crm_users;


-- hive.bi_silver_dev.cx_cso_customer_ticket_monthly source

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_cso_customer_ticket_monthly SECURITY DEFINER AS
SELECT
  product_category product_category
, (CASE WHEN (company_name IS NOT NULL) THEN regexp_replace(company_name, '^(.{2}).*(.{2})$', '$1****$2') ELSE null END) company_name
, DATE_TRUNC('month', created_at) report_month
, COUNT(DISTINCT ticket_id) ticket_count
, MAX(customer_group) customer_group
, MAX(customer_segment_l1) customer_segment_l1
, SUM(agent_reply_count) total_agent_reply_count
, SUM((CASE WHEN is_overdue THEN 1 ELSE 0 END)) overdue_ticket_count
, SUM((CASE WHEN (violated_level > 0) THEN 1 ELSE 0 END)) sla_violated_ticket_count
, AVG((CASE WHEN (resolved_at IS NOT NULL) THEN (date_diff('minute', created_at, resolved_at) / 60) END)) avg_resolve_time_minutes
FROM
  hive.bi_silver.cx_cso_support_tickets
GROUP BY 1, 2, 3;


-- hive.bi_silver_dev.cx_cso_support_tickets source

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_cso_support_tickets SECURITY DEFINER AS
SELECT
  ticket_id
, (CASE WHEN (subject IS NOT NULL) THEN concat(substr(subject, 5, 20), '***') ELSE null END) subject
, customer_group
, status_name
, priority_level
, created_at
, updated_at
, first_responded_at
, resolved_at
, closed_at
, call_reminder
, issue_category
, is_vcs
, l1
, l2
, l3
, l4
, is_duplicated_ticket
, is_reopened_by_cx
, (CASE WHEN (assigned_to IS NOT NULL) THEN concat(substr(assigned_to, 1, 5), '***') ELSE null END) assigned_to
, action_program
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 5), '***') ELSE null END) company_alias
, customer_satisfaction_rating
, customer_entry_channel
, customer_respond_date
, communication_effectiveness
, imported_ticket_date
, imported_ticket_due_date
, incident_root_cause
, issues_type
, (CASE WHEN (note IS NOT NULL) THEN '***MASKED***' ELSE null END) note
, number_of_due_date_changes
, old_due_date
, is_one_day_before_due
, is_one_hour_before_due
, reason
, related_ticket
, is_reminder_update
, is_notification
, is_overdue
, response_time_minutes
, sentiment
, severity_level
, spam_type
, support_category
, is_third_four_time
, ttr_overdue
, urgency_level
, resolution_time_minutes
, first_response_time_minutes
, l1_time_actual_minutes
, l1_time_allowed_minutes
, l1_violated
, l2_time_actual_minutes
, l2_time_allowed_minutes
, l2_violated
, l3_time_actual_minutes
, l3_time_allowed_minutes
, l3_violated
, l4_time_actual_minutes
, l4_time_allowed_minutes
, l4_violated
, time_to_response_minutes
, assigned_agent_stage
, violated_level
, (CASE WHEN (requester_name IS NOT NULL) THEN concat(substr(requester_name, 1, 5), '***') ELSE null END) requester_name
, company_name
, (CASE WHEN (agent_name IS NOT NULL) THEN concat(substr(agent_name, 1, 5), '***') ELSE null END) agent_name
, resolution_time_in_business_hours
, agent_reply_count
, first_response_date
, due_date_change_count
, tickets_first_responded_within_sla
, tickets_resolved_within_sla
, ttr_time_minutes
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, product_category_code
, product_category
, product_item_type
FROM
  hive.bi_silver.cx_cso_support_tickets;


-- hive.bi_silver_dev.cx_cso_ticket_tags source

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_cso_ticket_tags SECURITY DEFINER AS
SELECT
  ticket_id
, tag
, created_at
, updated_at
FROM
  hive.bi_silver.cx_cso_ticket_tags;


-- hive.bi_silver_dev.cx_sur_question_answer_choices source

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_sur_question_answer_choices SECURITY DEFINER AS
SELECT
  survey_id
, question_id
, question_type
, question_clean
, answer_choice_id
, answer_choice_content
, kpi_type
, journey
, customer_touchpoint
, product_category
FROM
  hive.bi_silver.cx_sur_question_answer_choices;


-- hive.bi_silver_dev.cx_sur_question_response source

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_sur_question_response SECURITY DEFINER AS
SELECT
  collected_date
, survey_id
, response_id
, respondent_uuid
, question_id
, question_type
, choice_id
, answer_string
, answer_tag
, (CASE WHEN (comment IS NOT NULL) THEN '***MASKED***' ELSE null END) comment
, choice_content
, item_content
, answer_number
, item_score_value
, item_score_label
, question_clean
, kpi_type
, journey
, customer_touchpoint
, product_category
, kpi_group
FROM
  hive.bi_silver.cx_sur_question_response;


-- hive.bi_silver_dev.cx_sur_surveys source

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_sur_surveys SECURITY DEFINER AS
SELECT
  survey_id
, survey_type
, survey_name
, folder_name
, created_at
, enabled
, journey
, customer_touchpoint
, product_category
FROM
  hive.bi_silver.cx_sur_surveys;


-- hive.bi_silver_dev.dim_customer_segment_l1 source

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_customer_segment_l1 SECURITY DEFINER AS
SELECT segment_l1
FROM
  hive.bi_silver.dim_customer_segment_l1;


-- hive.bi_silver_dev.dim_date source

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_date SECURITY DEFINER AS
SELECT
  date_ts
, date_key
, date_year
, date_quarter
, date_month
, date_week
, date_day
, date_day_of_week
, date_day_name
, date_month_name
, date_year_month
, date_year_week
, date_is_weekend
, date_is_month_end
, date_is_quarter_end
FROM
  hive.bi_silver.dim_date;


-- hive.bi_silver_dev.dim_product_category source

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_product_category SECURITY DEFINER AS
SELECT product_category
FROM
  hive.bi_silver.dim_product_category;


-- hive.bi_silver_dev.dim_territory_name source

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_territory_name SECURITY DEFINER AS
SELECT territory_name
FROM
  hive.bi_silver.dim_territory_name;


-- hive.bi_silver_dev.dim_unit_level_1 source

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_unit_level_1 SECURITY DEFINER AS
SELECT unit_level_1
FROM
  hive.bi_silver.dim_unit_level_1;


-- hive.bi_silver_dev.finance_actual_cost source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_actual_cost SECURITY DEFINER AS
SELECT
  report_date
, report_year
, report_month
, old_category
, product_category_code
, cost_group
, (CASE WHEN (base_currency_amount IS NOT NULL) THEN CAST((TRY_CAST(base_currency_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) base_currency_amount
, currency_code
, territory_name
, product_category
, business_unit_level_1
FROM
  hive.bi_silver.finance_actual_cost;


-- hive.bi_silver_dev.finance_allocated_revenue source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_allocated_revenue SECURITY DEFINER AS
SELECT
  report_date
, report_year
, report_month
, product_category_code
, product_category
, product_growth_type
, product_service_group
, (CASE WHEN (revenue_amount IS NOT NULL) THEN CAST((TRY_CAST(revenue_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) revenue_amount
, (CASE WHEN (company_alias IS NOT NULL) THEN concat(substr(company_alias, 1, 5), '***') ELSE null END) company_alias
, revenue_type
, channel_name
, is_soc
, department_alias
, customer_segment_l1
, customer_segment_l2
, customer_segment_l3
, revenue_group
, (CASE WHEN (vat IS NOT NULL) THEN CAST((TRY_CAST(vat AS DOUBLE) / 1000) AS BIGINT) ELSE null END) vat
, payment_stage
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 5), '***') ELSE null END) am_username
, (CASE WHEN (presale_name IS NOT NULL) THEN concat(substr(presale_name, 1, 5), '***') ELSE null END) presale_name
, currency_code
, business_unit_level_1
, territory_name
, deal_id
, update_at
, (CASE WHEN (currency_amount IS NOT NULL) THEN CAST((TRY_CAST(currency_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) currency_amount
, is_actual
, due_date
, deployment_type
, pricebook_id
, is_recurring
, product_index
, customer_id
, (CASE WHEN (customer_name IS NOT NULL) THEN concat(substr(customer_name, 1, 5), '***') ELSE null END) customer_name
, customer_group
, product_category_index
, contract_id
, payment_index
, contract_allocation_id
FROM
  hive.bi_silver.finance_allocated_revenue;


-- hive.bi_silver_dev.finance_contract_collected_invoices source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_contract_collected_invoices SECURITY DEFINER AS
SELECT
  idd
, sales_channel
, transaction_code
, invoice_number
, (CASE WHEN (contract_num IS NOT NULL) THEN regexp_replace(contract_num, '^(.{3}).*(.{2})$', '$1****$2') ELSE null END) contract_num
, customer_code
, contract_description
, (CASE WHEN (company_name IS NOT NULL) THEN concat(substr(company_name, 1, 3), '***') ELSE null END) company_name
, (CASE WHEN (original_amount IS NOT NULL) THEN CAST((TRY_CAST(original_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) original_amount
, (CASE WHEN (initial_receivable_amount IS NOT NULL) THEN CAST((TRY_CAST(initial_receivable_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) initial_receivable_amount
, (CASE WHEN (total_received_amount IS NOT NULL) THEN CAST((TRY_CAST(total_received_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) total_received_amount
, (CASE WHEN (remaining_balance_amount IS NOT NULL) THEN CAST((TRY_CAST(remaining_balance_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) remaining_balance_amount
, invoice_date
, payment_deadline
, payment_date
, overdue_months_numeric
, overdue_status_enum
, (CASE WHEN (am_username IS NOT NULL) THEN concat(substr(am_username, 1, 5), '***') ELSE null END) am_username
FROM
  hive.bi_silver.finance_contract_collected_invoices;


-- hive.bi_silver_dev.finance_cost_plan source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_cost_plan SECURITY DEFINER AS
SELECT
  plan_date
, plan_year
, plan_month
, cost_code
, cost_group
, (CASE WHEN (expected_cost_value IS NOT NULL) THEN CAST((TRY_CAST(expected_cost_value AS DOUBLE) / 1000) AS BIGINT) ELSE null END) expected_cost_value
, currency_code
FROM
  hive.bi_silver.finance_cost_plan;


-- hive.bi_silver_dev.finance_daily_business_metrics source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_daily_business_metrics SECURITY DEFINER AS
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
, COALESCE(product_category, 'Khác') product_category
, COALESCE(business_unit_level_1, 'Khác') business_unit_level_1
, COALESCE(territory_name, 'Khác') territory_name
, CAST((SUM(actual_cost_amount) / 1000) AS BIGINT) actual_cost_amount
, CAST((SUM(allocated_revenue_amount) / 1000) AS BIGINT) allocated_revenue_amount
, CAST((SUM(allocated_revenue_amount) / 1000) AS BIGINT) cash_collection_amount
, CAST((SUM(sales_revenue_amount) / 1000) AS BIGINT) sales_revenue_amount
, CAST((SUM(planned_revenue_must_amount) / 1000) AS BIGINT) planned_revenue_must_amount
, CAST((SUM(planned_revenue_nice_amount) / 1000) AS BIGINT) planned_revenue_nice_amount
, CAST(((SUM(sales_revenue_amount) - SUM(actual_cost_amount)) / 1000) AS BIGINT) gross_profit_amount
, CAST(((SUM(allocated_revenue_amount) - SUM(planned_revenue_must_amount)) / 1000) AS BIGINT) revenue_vs_plan_gap_amount
FROM
  fact_union
GROUP BY 1, 2, 3, 4;


-- hive.bi_silver_dev.finance_fact_ratios source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_fact_ratios SECURITY DEFINER AS
SELECT
  report_date
, metric_code
, metric_name
, metric_value
FROM
  hive.bi_silver.finance_fact_ratios;


-- hive.bi_silver_dev.finance_metrics source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_metrics SECURITY DEFINER AS
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
, product_category_code product_category_code
, product_category product_category
, business_unit_level_1 business_unit_level_1
, CAST((TRY_CAST(total_revenue AS DOUBLE) / 1000) AS BIGINT) total_revenue
, CAST((TRY_CAST(total_cost AS DOUBLE) / 1000) AS BIGINT) total_cost
, CAST(((total_revenue - (direct_labor_os_cost + production_infra_cost)) / 1000) AS BIGINT) gross_profit
, CAST((((total_revenue - total_cost) + fixed_asset_depreciation_cost) / 1000) AS BIGINT) ebitda
, CAST((TRY_CAST(fixed_asset_depreciation_cost AS DOUBLE) / 1000) AS BIGINT) fixed_asset_depreciation_cost
, CAST((TRY_CAST(direct_labor_os_cost AS DOUBLE) / 1000) AS BIGINT) direct_labor_os_cost
, CAST((TRY_CAST(production_infra_cost AS DOUBLE) / 1000) AS BIGINT) production_infra_cost
, CAST((TRY_CAST(business_os_cost AS DOUBLE) / 1000) AS BIGINT) business_os_cost
FROM
  final_join;


-- hive.bi_silver_dev.finance_product_revenue_plan source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_product_revenue_plan SECURITY DEFINER AS
SELECT
  plan_date
, plan_year
, plan_month
, (CASE WHEN (plan_must_amount IS NOT NULL) THEN CAST((TRY_CAST(plan_must_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) plan_must_amount
, (CASE WHEN (plan_should_amount IS NOT NULL) THEN CAST((TRY_CAST(plan_should_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) plan_should_amount
, (CASE WHEN (plan_nice_amount IS NOT NULL) THEN CAST((TRY_CAST(plan_nice_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) plan_nice_amount
, product_group
, product_category_code
, product_category
, product_name
, business_group_name
, sub_group_name
, currency_code
FROM
  hive.bi_silver.finance_product_revenue_plan;


-- hive.bi_silver_dev.finance_production_cost_allocations source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_production_cost_allocations SECURITY DEFINER AS
SELECT
  report_date
, report_year
, report_month
, old_category
, old_product_category
, product_category_code
, cost_group
, (CASE WHEN (base_currency_amount IS NOT NULL) THEN CAST((TRY_CAST(base_currency_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) base_currency_amount
, currency_code
, territory_name
, expense_code
, market_name
FROM
  hive.bi_silver.finance_production_cost_allocations;


-- hive.bi_silver_dev.finance_revenue_plan source

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_revenue_plan SECURITY DEFINER AS
SELECT
  plan_date
, plan_year
, plan_month
, (CASE WHEN (plan_viettel_group_amount IS NOT NULL) THEN CAST((TRY_CAST(plan_viettel_group_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) plan_viettel_group_amount
, (CASE WHEN (plan_must_amount IS NOT NULL) THEN CAST((TRY_CAST(plan_must_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) plan_must_amount
, (CASE WHEN (plan_nice_amount IS NOT NULL) THEN CAST((TRY_CAST(plan_nice_amount AS DOUBLE) / 1000) AS BIGINT) ELSE null END) plan_nice_amount
, segment_l1_alias
, currency_code
, segment_l1
, customer_segment_l1
, segment_l2
, customer_segment_l2
FROM
  hive.bi_silver.finance_revenue_plan;


-- hive.bi_silver_dev.giang_test_masterplan source

CREATE OR REPLACE VIEW hive.bi_silver_dev.giang_test_masterplan SECURITY DEFINER AS
SELECT
  plan_date
, category_code
, product_category
, customer_segment_l1
, customer_segment_l2
, plan_viettel_group
, plan_must
, plan_should
, plan_nice
FROM
  hive.bi_silver.giang_test_masterplan;


-- hive.bi_silver_dev.hr_employee_headcount source

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_employee_headcount SECURITY DEFINER AS
SELECT
  headcount_plan
, headcount_status
, division_n
, unit_n_1
, department_n_2
, department_n_3
, role_base
, specialization
, required_level
, position_status
, (CASE WHEN (full_name IS NOT NULL) THEN concat(substr(full_name, 1, 1), '***') ELSE null END) full_name
, employee_code
, employee_object
, contract_type
, hire_date_vcs
, level
, region
, management_level
, branch
, (CASE WHEN (business_email IS NOT NULL) THEN regexp_replace(business_email, '^(.{2}).+(@.+)$', '$1***$2') ELSE null END) business_email
, job_framework_position
, job_framework_position_2
, service_group
, service_group_new
, is_ai_group
, is_ai_org
, is_philippines_japan_market
, is_domestic_business
, is_international_business
, is_rnd
, is_indirect_group
, is_business_support
FROM
  hive.bi_silver.hr_employee_headcount;


-- hive.bi_silver_dev.hr_employee_onboard source

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_employee_onboard SECURITY DEFINER AS
SELECT
  employee_code
, (CASE WHEN (full_name IS NOT NULL) THEN concat(substr(full_name, 1, 1), '***') ELSE null END) full_name
, current_status
, recruitment_status
, employee_type
, contract_company_independent
, branch
, division
, unit_level_1
, business_unit_level_1
, department_level_2
, role_base
, job_level
, technical_grade_band
, technical_grade_level
, management_level
, hire_date
, hire_date_viettel
, termination_date
, (CASE WHEN (business_email IS NOT NULL) THEN regexp_replace(business_email, '^(.{2}).+(@.+)$', '$1***$2') ELSE null END) business_email
, email_nametag
, date_of_birth
, gender
, pr_q1_2024
, pr_q2_2024
, pr_q3_2024
, pr_q4_2024
, pr_q1_2025
, pr_q2_2025
, pr_q3_2025
, achievement_title_2024
, talent_group
, performance_level
, potential_level
, termination_status
, termination_reason_1
, termination_reason_2
, termination_detail_reason
, next_workplace
, is_new_hire
, age
, age_group
, department_level_3
, key_core_group
, is_key_employee
, employee_criticality_level
FROM
  hive.bi_silver.hr_employee_onboard;


-- hive.bi_silver_dev.hr_employee_resigned source

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_employee_resigned SECURITY DEFINER AS
SELECT
  employee_code
, (CASE WHEN (full_name IS NOT NULL) THEN concat(substr(full_name, 1, 1), '***') ELSE null END) full_name
, current_status
, (CASE WHEN (business_email IS NOT NULL) THEN regexp_replace(business_email, '^(.{2}).+(@.+)$', '$1***$2') ELSE null END) business_email
, email_nametag
, recruitment_status
, employee_type
, contract_company_independent
, branch
, division
, unit_level_1
, business_unit_level_1
, department_level_2
, department_n_3
, role_base
, job_level
, region
, management_level
, hire_date
, hire_date_vcs
, termination_date
, date_of_birth
, gender
, seniority_group
, pr_q1_2024
, pr_q2_2024
, pr_q3_2024
, pr_q4_2024
, pr_q1_2025
, pr_q2_2025
, achievement_2024
, talent_9box_group
, performance_level
, potential_level
, leave_status
, resignation_reason_level_1
, resignation_reason_level_2
, resignation_reason_detail
, next_workplace_score
, employment_status
, termination_reason
, is_key_employee
, is_high_potential
FROM
  hive.bi_silver.hr_employee_resigned;


-- hive.bi_silver_dev.hr_master_report source

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_master_report SECURITY DEFINER AS
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
, (CASE WHEN (email_nametag IS NOT NULL) THEN concat(substr(email_nametag, 1, 2), '***') ELSE null END) email_username
, current_status current_status
, division division
, business_unit_level_1 business_unit_level_1
, department_level_2 department_level_2
, job_level job_level
, management_level management_level
, employee_type
, CAST(hire_date AS timestamp) hire_date
, CAST(termination_date_r AS timestamp) termination_date
, gender
, CAST(((year(current_date) - year(date_of_birth)) - (CASE WHEN (date_format(current_date, 'MM-dd') < date_format(date_of_birth, 'MM-dd')) THEN 1 ELSE 0 END)) AS INT) age
, COALESCE(employment_status, 'ACTIVE') employment_status
, termination_reason
, talent_group talent_group
, performance_level performance_level
, potential_level potential_level
, (CASE WHEN (is_key_employee = 1) THEN 1 ELSE 0 END) is_key_employee_flag
FROM
  ranked_hr
WHERE (rn = 1);


-- hive.bi_silver_dev.jira_msn source

CREATE OR REPLACE VIEW hive.bi_silver_dev.jira_msn SECURITY DEFINER AS
SELECT
  key
, start_date
, summary
, issuetype
, status
, (CASE WHEN (assignee IS NOT NULL) THEN concat(substr(assignee, 1, 2), '***') ELSE null END) assignee
, projectkey
, department_center
, task_type
, duedate
, created
, updated
FROM
  hive.bi_silver.jira_msn;


-- hive.bi_silver_dev.jira_task_operation source

CREATE OR REPLACE VIEW hive.bi_silver_dev.jira_task_operation SECURITY DEFINER AS
SELECT
  key
, start_date
, due_date
, summary
, issue_type
, priority
, status
, (CASE WHEN (assignor IS NOT NULL) THEN concat(substr(assignor, 1, 2), '***') ELSE null END) assignor
, work_group
, department_center
, (CASE WHEN (assignee IS NOT NULL) THEN concat(substr(assignee, 1, 2), '***') ELSE null END) assignee
, created
, updated
FROM
  hive.bi_silver.jira_task_operation;


-- hive.bi_silver_dev.noc_entities source

CREATE OR REPLACE VIEW hive.bi_silver_dev.noc_entities SECURITY DEFINER AS
SELECT
  customer
, project
, (CASE WHEN (host IS NOT NULL) THEN regexp_replace(host, '^(.{3}).*(.{3})$', '$1****$2') ELSE null END) host
FROM
  hive.bi_silver.noc_entities;


-- hive.bi_silver_dev.noc_metrics source

CREATE OR REPLACE VIEW hive.bi_silver_dev.noc_metrics SECURITY DEFINER AS
SELECT
  metric_name
, agg_type
, customer
, project
, (CASE WHEN (host IS NOT NULL) THEN regexp_replace(host, '^(.{3}).*(.{3})$', '$1****$2') ELSE null END) host
, metric_value
, metric_ts
, metric_time
, dt
FROM
  hive.bi_silver.noc_metrics;

  CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_activity_history
SECURITY DEFINER
AS
SELECT
    id,

    CASE
        WHEN activity_name IS NOT NULL
        THEN concat(substr(activity_name,1,5),'***')
        ELSE NULL
    END AS activity_name,

    am_user_id,
    contact_id,
    company_id,
    deal_id,
    created_at,
    creator_id,
    updater_id,
    updated_at,

    CASE
        WHEN recent_note IS NOT NULL
        THEN concat(substr(recent_note,1,20),'***')
        ELSE NULL
    END AS recent_note,

    record_type_id

FROM hive.bi_silver.crm_activity_history;


CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_business_rules
SECURITY DEFINER
AS
SELECT
    id,

    CASE
        WHEN business_rule_name IS NOT NULL
        THEN concat(substr(business_rule_name,1,5),'***')
        ELSE NULL
    END AS business_rule_name,

    am_user_id,
    rule_type,
    price_type,
    parent_product_id,
    child_product_id,
    ratio_operator,
    ratio_value,
    ratio_unit,
    is_active,
    created_at,
    creator_id,
    updated_at,
    updater_id,
    record_type_id

FROM hive.bi_silver.crm_business_rules;

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_performance_rate
SECURITY DEFINER
AS
SELECT

    CASE
        WHEN employee_id IS NOT NULL
        THEN concat(substr(employee_id,1,5),'***')
        ELSE NULL
    END AS employee_id,

    CASE
        WHEN employee_name IS NOT NULL
        THEN concat(substr(employee_name,1,5),'***')
        ELSE NULL
    END AS employee_name,

    CASE
        WHEN username IS NOT NULL
        THEN concat(substr(username,1,5),'***')
        ELSE NULL
    END AS username,

    CASE
        WHEN business_email IS NOT NULL
        THEN to_hex(md5(to_utf8(lower(business_email))))
        ELSE NULL
    END AS business_email,

    gender,
    age_group,
    branch_name,
    contract_company_type,
    evaluation_date,
    hire_date_vcs,
    termination_date,
    job_title,
    tenure,
    n1_group,
    n2_group,
    employee_level,
    score,
    ki,
    crawled_at_ts

FROM hive.bi_silver.hr_performance_rate;



drop view hive.bi_silver_dev.crm_vw_estimated_revenue;

CREATE or REPLACE VIEW hive.bi_silver_dev.crm_vw_estimated_revenue
AS

SELECT
    report_date,
    report_year,
    report_month,
    product_category,
    am_username,
    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,
    customer_group,
    CASE
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
        END AS customer_segment,
    'DTCC' AS kpi_name,
    revenue_amount AS revenue_amount 
FROM hive.bi_silver.crm_allocated_revenue 

UNION ALL

SELECT
    payment_date AS report_date,
    year(payment_date) AS report_year,
    month(payment_date) AS report_month,
    product_category,
    am_username,
    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,
    customer_group,
    CASE
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
        END AS customer_segment,
    'DTPV' AS kpi_name,
    vnd_amount AS revenue_amount
FROM hive.bi_silver.crm_payment_forecast




DROP View hive.bi_silver_dev.crm_vw_mart_revenue_segment_month;

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_vw_mart_revenue_segment_month AS

WITH revenue AS (

    SELECT
        report_year,
        report_month,
        customer_segment_l1,
        customer_segment_l2,

        SUM(dtcc_amount) AS dtcc_amount,
        SUM(dtpv_amount) AS dtpv_amount,

        SUM(arr_committed_amount) AS arr_committed_amount,
        SUM(arr_forecast_amount) AS arr_forecast_amount

    FROM (

        SELECT
            report_year,
            report_month,
            customer_segment_l1,
            customer_segment_l2,

            revenue_amount AS dtcc_amount,
            0 AS dtpv_amount,

            CASE
                WHEN is_recurring THEN revenue_amount
                ELSE 0
            END AS arr_committed_amount,

            0 AS arr_forecast_amount

        FROM hive.bi_silver.crm_allocated_revenue

        UNION ALL

        SELECT
            year(payment_date) AS report_year,
            month(payment_date) AS report_month,
            customer_segment_l1,
            customer_segment_l2,

            0 AS dtcc_amount,
            vnd_amount AS dtpv_amount,

            0 AS arr_committed_amount,

            CASE
                WHEN is_recurring THEN vnd_amount
                ELSE 0
            END AS arr_forecast_amount

        FROM hive.bi_silver.crm_payment_forecast

    ) t

    GROUP BY
        report_year,
        report_month,
        customer_segment_l1,
        customer_segment_l2
),

plan AS (

    SELECT
        plan_year,
        plan_month,
        customer_segment_l1,
        customer_segment_l2,
        SUM(plan_must_amount) AS plan_must_amount,
        SUM(plan_nice_amount) AS plan_nice_amount,
        SUM(plan_viettel_group_amount) AS plan_viettel_group_amount

    FROM hive.bi_silver.finance_revenue_plan

    GROUP BY
        plan_year,
        plan_month,
        customer_segment_l1,
        customer_segment_l2
)

SELECT

    COALESCE(r.report_year,p.plan_year) AS report_year,
    COALESCE(r.report_month,p.plan_month) AS report_month,

    COALESCE(r.customer_segment_l1,p.customer_segment_l1)
        AS customer_segment_l1,

    COALESCE(r.customer_segment_l2,p.customer_segment_l2)
        AS customer_segment_l2,

    CASE
               WHEN COALESCE(r.customer_segment_l1,p.customer_segment_l1) = 'Nội bộ'
               OR (
                     COALESCE(r.customer_segment_l1,p.customer_segment_l1) = 'International'
                  AND COALESCE(r.customer_segment_l2,p.customer_segment_l2) = 'Direct / Local Channel'
               )
                  THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'

               WHEN COALESCE(r.customer_segment_l1,p.customer_segment_l1) = 'International'
               AND COALESCE(r.customer_segment_l2,p.customer_segment_l2) = 'Viettel Global Partner'
                  THEN 'DT Quốc tế (KH ngoài QT)'

               WHEN COALESCE(r.customer_segment_l1,p.customer_segment_l1) = 'Bộ Quốc phòng'
                  THEN 'DT BQP (gồm cả SI BQP)'

               WHEN COALESCE(r.customer_segment_l1,p.customer_segment_l1) = 'Khách hàng ngoài'
                  THEN 'DT ngoài trong nước (gồm cả SI ngoài)'

               ELSE 'Khác'
         END AS customer_segment,

    COALESCE(dtcc_amount,0)
        AS committed_revenue_amount,

    COALESCE(dtpv_amount,0)
        AS forecast_revenue_amount,

    COALESCE(dtcc_amount,0)
      + COALESCE(dtpv_amount,0)
        AS estimated_revenue_amount,

    COALESCE(arr_committed_amount,0)
        AS arr_committed_revenue_amount,

    COALESCE(arr_forecast_amount,0)
        AS arr_forecast_revenue_amount,

    COALESCE(arr_committed_amount,0)
      + COALESCE(arr_forecast_amount,0)
        AS arr_total_revenue_amount,

    COALESCE(plan_must_amount,0)
        AS must_plan_amount,

    COALESCE(plan_nice_amount,0)
        AS nice_plan_amount,

    COALESCE(plan_viettel_group_amount,0)
        AS viettel_group_plan_amount

FROM revenue r
FULL OUTER JOIN plan p
    ON r.report_year = p.plan_year
   AND r.report_month = p.plan_month
   AND r.customer_segment_l1 = p.customer_segment_l1
   AND r.customer_segment_l2 = p.customer_segment_l2;


   


DROP View hive.bi_silver_dev.crm_vw_mart_revenue_am_month;

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_vw_mart_revenue_am_month AS
WITH revenue as (
SELECT
        report_year,
        report_month,
        customer_segment_l3 as vvip_am,
		'AM Nội Bộ' as internal_am,
		channel_name as channel_am,
        SUM(dtcc_amount) AS dtcc_amount,
        SUM(dtpv_amount) AS dtpv_amount,

        SUM(arr_committed_amount) AS arr_committed_amount,
        SUM(arr_forecast_amount) AS arr_forecast_amount
		
		 
    FROM (

        SELECT
            report_year,
            report_month,
            customer_segment_l3,

			'AM Nội Bộ' as internal_am,
			channel_name,
            revenue_amount AS dtcc_amount,
            0 AS dtpv_amount,

            CASE
                WHEN is_recurring THEN revenue_amount
                ELSE 0
            END AS arr_committed_amount,

            0 AS arr_forecast_amount

        FROM hive.bi_silver.crm_allocated_revenue

        UNION ALL

        SELECT
            year(payment_date) AS report_year,
            month(payment_date) AS report_month,
            customer_segment_l3,
			'AM Nội Bộ' as internal_am,
			'N/A' as  channel_name,
            0 AS dtcc_amount,
            vnd_amount AS dtpv_amount,

            0 AS arr_committed_amount,

            CASE
                WHEN is_recurring THEN vnd_amount
                ELSE 0
            END AS arr_forecast_amount

        FROM hive.bi_silver.crm_payment_forecast

    ) t

    GROUP BY
        report_year,
        report_month,
        customer_segment_l3,
		internal_am,
		channel_name
)
select         
		report_year,
        report_month,
        vvip_am,
		internal_am,
		channel_am,

     COALESCE(dtcc_amount,0)
        AS committed_revenue_amount,

    COALESCE(dtpv_amount,0)
        AS forecast_revenue_amount,

    COALESCE(dtcc_amount,0)
      + COALESCE(dtpv_amount,0)
        AS estimated_revenue_amount,

    COALESCE(arr_committed_amount,0)
        AS arr_committed_revenue_amount,

    COALESCE(arr_forecast_amount,0)
        AS arr_forecast_revenue_amount,

    COALESCE(arr_committed_amount,0)
      + COALESCE(arr_forecast_amount,0)
        AS arr_total_revenue_amount
		
FROM revenue r


CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_mart_estimated_revenue
SECURITY DEFINER AS
SELECT
    report_date,
    product_category,
    customer_segment_l1,
    customer_segment_l2,
    am_group,
    customer_group,
    customer_segment,

    CAST(TRY_CAST(carryover_renewal_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS carryover_renewal_revenue_amount,
    CAST(TRY_CAST(in_year_renewal_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS in_year_renewal_revenue_amount,
    CAST(TRY_CAST(carryover_new_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS carryover_new_revenue_amount,
    CAST(TRY_CAST(in_year_new_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS in_year_new_revenue_amount,

    CAST(TRY_CAST(committed_non_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_non_si_revenue_amount,
    CAST(TRY_CAST(committed_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_si_revenue_amount,
    CAST(TRY_CAST(committed_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_arr_amount,
    CAST(TRY_CAST(committed_none_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_none_arr_amount,

    CAST(TRY_CAST(renewal_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS renewal_revenue_amount,
    CAST(TRY_CAST(new_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS new_revenue_amount,
    CAST(TRY_CAST(committed_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_revenue_amount,

    CAST(TRY_CAST(forecast_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_revenue_amount,
    CAST(TRY_CAST(pipeline_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS pipeline_revenue_amount,

    CAST(TRY_CAST(forecast_non_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_non_si_revenue_amount,
    CAST(TRY_CAST(forecast_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_si_revenue_amount,
    CAST(TRY_CAST(forecast_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_arr_amount,
    CAST(TRY_CAST(forecast_non_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_non_arr_amount,

    CAST(TRY_CAST(estimated_non_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS estimated_non_si_revenue_amount,
    CAST(TRY_CAST(estimated_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS estimated_si_revenue_amount,
    CAST(TRY_CAST(estimated_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS estimated_revenue_amount,

    CAST(TRY_CAST(arr_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS arr_revenue_amount

FROM hive.bi_silver.crm_mart_estimated_revenue;

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_mart_revenue_performance_month
SECURITY DEFINER AS
SELECT
    report_date,
    customer_segment_l1,
    customer_segment_l2,
    report_customer_segment,

    CAST(TRY_CAST(carryover_renewal_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS carryover_renewal_revenue_amount,
    CAST(TRY_CAST(in_year_renewal_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS in_year_renewal_revenue_amount,
    CAST(TRY_CAST(carryover_new_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS carryover_new_revenue_amount,
    CAST(TRY_CAST(in_year_new_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS in_year_new_revenue_amount,

    CAST(TRY_CAST(committed_non_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_non_si_revenue_amount,
    CAST(TRY_CAST(committed_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_si_revenue_amount,
    CAST(TRY_CAST(committed_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_arr_amount,
    CAST(TRY_CAST(committed_none_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_none_arr_amount,

    CAST(TRY_CAST(renewal_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS renewal_revenue_amount,
    CAST(TRY_CAST(new_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS new_revenue_amount,
    CAST(TRY_CAST(committed_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS committed_revenue_amount,

    CAST(TRY_CAST(forecast_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_revenue_amount,
    CAST(TRY_CAST(pipeline_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS pipeline_revenue_amount,

    CAST(TRY_CAST(forecast_non_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_non_si_revenue_amount,
    CAST(TRY_CAST(forecast_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_si_revenue_amount,
    CAST(TRY_CAST(forecast_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_arr_amount,
    CAST(TRY_CAST(forecast_non_arr_amount AS DOUBLE) / 1000 AS BIGINT) AS forecast_non_arr_amount,

    CAST(TRY_CAST(estimated_non_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS estimated_non_si_revenue_amount,
    CAST(TRY_CAST(estimated_si_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS estimated_si_revenue_amount,
    CAST(TRY_CAST(estimated_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS estimated_revenue_amount,

    CAST(TRY_CAST(arr_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS arr_revenue_amount,

    CAST(TRY_CAST(must_plan_amount AS DOUBLE) / 1000 AS BIGINT) AS must_plan_amount,
    CAST(TRY_CAST(nice_plan_amount AS DOUBLE) / 1000 AS BIGINT) AS nice_plan_amount,
    CAST(TRY_CAST(viettel_group_plan_amount AS DOUBLE) / 1000 AS BIGINT) AS viettel_group_plan_amount

FROM hive.bi_silver.crm_mart_revenue_performance_month;

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_revenue_reconciliation
SECURITY DEFINER AS
SELECT
    invoice_date,
    report_month,
    billing_type,
    transaction_description,

    CAST(TRY_CAST(revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS revenue_amount,
    CAST(TRY_CAST(shared_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS shared_revenue_amount,
    CAST(TRY_CAST(actual_revenue_amount AS DOUBLE) / 1000 AS BIGINT) AS actual_revenue_amount,
    CAST(TRY_CAST(vat_amount AS DOUBLE) / 1000 AS BIGINT) AS vat_amount,
    CAST(TRY_CAST(gross_amount AS DOUBLE) / 1000 AS BIGINT) AS gross_amount,
    CAST(TRY_CAST(shared_revenue_from_mss_amount AS DOUBLE) / 1000 AS BIGINT) AS shared_revenue_from_mss_amount,

    revenue_type,
    service_category,
    business_type,
    customer_classification,
    customer_channel,

    CASE
        WHEN customer_name IS NOT NULL
        THEN concat(substr(customer_name, 1,5), '***')
        ELSE NULL
    END AS customer_name,

    segment,
    department_name,
    customer_group,

    CASE
        WHEN account_manager_name IS NOT NULL
        THEN concat(substr(account_manager_name, 1, 5), '***')
        ELSE NULL
    END AS account_manager_name,

    CASE
        WHEN presale_name IS NOT NULL
        THEN concat(substr(presale_name, 1, 5), '***')
        ELSE NULL
    END AS presale_name,

    service_offering_name,
    revenue_classification,
    soc_classification,
    market_segment,
    service_offering_code

FROM hive.bi_silver.crm_revenue_reconciliation;