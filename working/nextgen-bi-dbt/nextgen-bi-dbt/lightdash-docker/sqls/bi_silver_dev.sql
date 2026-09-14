SHOW SCHEMAS FROM hive;
CREATE SCHEMA hive.bi_silver_dev 
with (location='s3a://bi-silver-dev/');

-- hive.bi_silver_dev.company_contacts definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.company_contacts
SECURITY DEFINER
AS
SELECT
    company_id,
    company_name,
    source_system,

    contact_id,

    -- Mask full contact name
    CASE
        WHEN contact_name IS NOT NULL THEN '***MASKED***'
        ELSE NULL
    END AS contact_name,

    -- Mask first name
    CASE
        WHEN first_name IS NOT NULL THEN substr(first_name, 1, 1) || '***'
        ELSE NULL
    END AS first_name,

    -- Mask last name
    CASE
        WHEN last_name IS NOT NULL THEN substr(last_name, 1, 1) || '***'
        ELSE NULL
    END AS last_name,

    -- Partial email masking
    CASE
        WHEN email IS NOT NULL THEN regexp_replace(
            email,
            '^(.{2}).+(@.+)$',
            '$1***$2'
        )
        ELSE NULL
    END AS email,

    email_domain,

    created_at,
    updated_at
FROM hive.bi_silver.company_contacts;

-- hive.bi_silver.contract_collected_invoices definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.contract_collected_invoices
SECURITY DEFINER
AS
SELECT
    idd,
    sales_channel,
    transaction_code,

    -- Invoice number partial masking
    CASE
        WHEN invoice_number IS NOT NULL
        THEN regexp_replace(invoice_number, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS invoice_number,

    -- Contract number partial masking
    CASE
        WHEN contract_num IS NOT NULL
        THEN regexp_replace(contract_num, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_num,

    -- Customer code partial masking
    CASE
        WHEN customer_code IS NOT NULL
        THEN regexp_replace(customer_code, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS customer_code,

    contract_description,

    -- Company name masking
    CASE
        WHEN company_name IS NOT NULL
        THEN substr(company_name, 1, 3) || '***'
        ELSE NULL
    END AS company_name,

    original_currency,

    -- Monetary masking (dev-safe): keep coarse magnitude only
    CASE
        WHEN initial_receivable_amount IS NOT NULL THEN CAST(TRY_CAST(initial_receivable_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS initial_receivable_amount,
    CASE
        WHEN total_received_amount IS NOT NULL THEN CAST(TRY_CAST(total_received_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS total_received_amount,
    CASE
        WHEN remaining_balance_amount IS NOT NULL THEN CAST(TRY_CAST(remaining_balance_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS remaining_balance_amount,

    invoice_date,
    payment_deadline,
    payment_date,

    overdue_months_numeric,
    overdue_status_enum,

    -- Username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username

FROM hive.bi_silver.contract_collected_invoices;

-- hive.bi_silver.crm_contract_allocations definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_contract_allocations
SECURITY DEFINER
AS
SELECT
    deal_id,
    deal_name,
    deal_stage_name,

    -- Contract ID partial masking
    CASE
        WHEN contract_id IS NOT NULL
        THEN regexp_replace(contract_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_id,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,
    customer_group,

    company_id,
    
    -- Company name masking
    CASE
        WHEN company_name IS NOT NULL
        THEN substr(company_name, 1, 3) || '***'
        ELSE NULL
    END AS company_name,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    currency_code,

    -- Username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username,

    due_date,
    fac_date,

    pricebook_id,
    product_category_index,
    product_description,
    product_category,
    product_type,
    deployment_type,
    is_recurring,
    product_index,
    territory_name,

    -- Monetary masking (dev-safe): keep coarse magnitude only
    CASE
        WHEN product_total_value IS NOT NULL THEN CAST(TRY_CAST(product_total_value AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS product_total_value,
    CASE
        WHEN sales_performance_value IS NOT NULL THEN CAST(TRY_CAST(sales_performance_value AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS sales_performance_value,
    CASE
        WHEN period_value IS NOT NULL THEN CAST(TRY_CAST(period_value AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS period_value,

    period_number,
    period_name,

    invoice_activation_date,

    created_at,
    updated_at,
    closed_date,

    days_since_last_update,

    -- Sale admin masking
    CASE
        WHEN sale_admin IS NOT NULL
        THEN substr(sale_admin, 1, 2) || '***'
        ELSE NULL
    END AS sale_admin,

    first_payment_date,

    id,
    period_index

FROM hive.bi_silver.crm_contract_allocations;

-- hive.bi_silver.crm_contracts definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_contracts
SECURITY DEFINER
AS
SELECT
    id,

    -- Contract name masking
    CASE
        WHEN name IS NOT NULL
        THEN substr(name, 1, 5) || '***'
        ELSE NULL
    END AS name,

    -- User AM masking
    CASE
        WHEN user_am_id IS NOT NULL
        THEN substr(user_am_id, 1, 2) || '***'
        ELSE NULL
    END AS user_am_id,

    deal_id,

    -- Contract number masking
    CASE
        WHEN contract_number IS NOT NULL
        THEN regexp_replace(contract_number, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_number,

    contract_type,
    signing_method,
    signing_method_code,

    sale_account_id,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    -- Customer name masking
    CASE
        WHEN customer_name IS NOT NULL
        THEN substr(customer_name, 1, 3) || '***'
        ELSE NULL
    END AS customer_name,

    -- Tax code hashing
    CASE
        WHEN tax_code IS NOT NULL
        THEN to_hex(md5(to_utf8(tax_code)))
        ELSE NULL
    END AS tax_code,

    customer_group_vi,

    segment_l1,
    customer_segment_l1,

    segment_l2,
    customer_segment_l2,

    segment_l3,
    customer_segment_l3,

    status,
    status_code,
    status_vi,

    customer_group,

    is_vip_customer,
    is_enterprise_customer,
    is_state_owned,
    is_private_enterprise,
    is_banking_group,
    is_international_client,
    is_internal_client,

    sign_date,
    fac_date,

    duration,
    expire_date,

    usd_exchange_rate,
    currency_code,

    -- Monetary masking (dev-safe): keep coarse magnitude only
    CASE
        WHEN contract_amount IS NOT NULL THEN CAST(TRY_CAST(contract_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS contract_amount,
    CASE
        WHEN vat IS NOT NULL THEN CAST(TRY_CAST(vat AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vat,
    CASE
        WHEN vcs_contract_amount IS NOT NULL THEN CAST(TRY_CAST(vcs_contract_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vcs_contract_amount,
    CASE
        WHEN viettel_contract_amount IS NOT NULL THEN CAST(TRY_CAST(viettel_contract_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS viettel_contract_amount,

    -- Partner ID masking
    CASE
        WHEN partner_id IS NOT NULL
        THEN regexp_replace(partner_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS partner_id

FROM hive.bi_silver.crm_contracts;

-- hive.bi_silver.crm_deal_interested_products definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_interested_products
SECURITY DEFINER
AS
SELECT
    deal_id,
    created_at,
    interested_product_category
FROM hive.bi_silver.crm_deal_interested_products;

-- hive.bi_silver.crm_deal_quotation_products definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_quotation_products
SECURITY DEFINER
AS
SELECT
    deal_id,

    -- Quotation ID partial masking
    CASE
        WHEN quotation_id IS NOT NULL
        THEN regexp_replace(quotation_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS quotation_id,

    product_line_no,

    -- Product ID partial masking
    CASE
        WHEN product_id IS NOT NULL
        THEN regexp_replace(product_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS product_id,

    quotation_created_at,
    quotation_status,
    quotation_lang,

    currency_code,

    global_discount,
    global_discount_type,

    product_name,

    base_price,
    quantitative,

    unit,
    product_unit,

    duration,
    package,

    CASE
        WHEN vat IS NOT NULL THEN CAST(TRY_CAST(vat AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vat,

    CASE
        WHEN discount_amount IS NOT NULL THEN CAST(TRY_CAST(discount_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS  discount_amount,
    discount_type,

    CASE
        WHEN final_total_amount IS NOT NULL THEN CAST(TRY_CAST(final_total_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS final_total_amount,
    CASE
        WHEN base_total_amount IS NOT NULL THEN CAST(TRY_CAST(base_total_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS base_total_amount,

    id

FROM hive.bi_silver.crm_deal_quotation_products;

-- hive.bi_silver.crm_deal_quotations definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_quotations
SECURITY DEFINER
AS
SELECT
    deal_id,

    -- Quotation ID partial masking
    CASE
        WHEN quotation_id IS NOT NULL
        THEN regexp_replace(quotation_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS quotation_id,

    created_at,

    status,

    tlang,

    currency_code,

    global_discount,
    global_discount_type

FROM hive.bi_silver.crm_deal_quotations;

-- hive.bi_silver.crm_deal_reasons definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deal_reasons
SECURITY DEFINER
AS
SELECT
    id,

    -- reason_name: treat as business-sensitive text, apply light masking
    CASE
        WHEN reason_name IS NOT NULL
        THEN substr(reason_name, 1, 3) || '***'
        ELSE NULL
    END AS reason_name,

    position,
    is_partial
FROM hive.bi_silver.crm_deal_reasons;

-- hive.bi_silver.crm_deals definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_deals
SECURITY DEFINER
AS
SELECT
    deal_id,

    -- Deal name masking
    CASE
        WHEN deal_name IS NOT NULL
        THEN substr(deal_name, 1, 5) || '***'
        ELSE NULL
    END AS deal_name,

    -- Monetary masking (dev-safe): keep coarse magnitude only
    CASE
        WHEN currency_amount IS NOT NULL THEN CAST(TRY_CAST(currency_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS currency_amount,
    CASE
        WHEN vnd_amount IS NOT NULL THEN CAST(TRY_CAST(vnd_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vnd_amount,

    expected_close_date,
    closed_date,
    stage_updated_time,

    days_to_close,
    days_since_last_update,

    is_deal_locked,

    usd_to_vnd,

    get_live_date,

    periodicity,
    is_select_viettel,
    has_budget,

    company_id,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    CASE
        WHEN budget_amount IS NOT NULL THEN CAST(TRY_CAST(budget_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS budget_amount,

    customer_group,
    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    channel,

    opportunity_weight_pct,
    bidding_required,

    -- Presales masking
    CASE
        WHEN presales_name IS NOT NULL
        THEN substr(presales_name, 1, 2) || '***'
        ELSE NULL
    END AS presales_name,

    -- Project manager masking
    CASE
        WHEN project_manager IS NOT NULL
        THEN substr(project_manager, 1, 2) || '***'
        ELSE NULL
    END AS project_manager,

    -- Sales admin masking
    CASE
        WHEN sales_admin IS NOT NULL
        THEN substr(sales_admin, 1, 2) || '***'
        ELSE NULL
    END AS sales_admin,

    -- Partner ID masking
    CASE
        WHEN partner_id IS NOT NULL
        THEN regexp_replace(partner_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS partner_id,

    -- Contract ID masking
    CASE
        WHEN contract_id IS NOT NULL
        THEN regexp_replace(contract_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_id,

    initial_source,
    warm_up_source,

    territory_name,

    contract_duration_month,
    is_created_contract,

    expire_date,

    -- Company name masking
    CASE
        WHEN company_name IS NOT NULL
        THEN substr(company_name, 1, 3) || '***'
        ELSE NULL
    END AS company_name,

    -- Company address masking
    CASE
        WHEN company_address IS NOT NULL
        THEN substr(company_address, 1, 10) || '***'
        ELSE NULL
    END AS company_address,

    company_province,
    company_country,

    -- Company email masking
    CASE
        WHEN company_email IS NOT NULL
        THEN regexp_replace(
            company_email,
            '^(.{2}).+(@.+)$',
            '$1***$2'
        )
        ELSE NULL
    END AS company_email,

    -- Company phone masking
    CASE
        WHEN company_phone IS NOT NULL
        THEN regexp_replace(
            company_phone,
            '(\\d{3})\\d{4}(\\d+)',
            '$1****$2'
        )
        ELSE NULL
    END AS company_phone,

    -- Company contact masking
    CASE
        WHEN company_contact IS NOT NULL
        THEN substr(company_contact, 1, 1) || '***'
        ELSE NULL
    END AS company_contact,

    currency_code,
    quotation_status,

    fac_date,

    -- AM username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username,

    check_change,
    probability,

    updated_at,
    created_at,

    deal_stage_id,
    deal_status,
    deal_payment_status_id,

    age,

    -- Recent note masking
    CASE
        WHEN recent_note IS NOT NULL
        THEN '***MASKED***'
        ELSE NULL
    END AS recent_note,

    next_scheduled_activity_time,
    last_assigned_at,

    last_contacted_activity_status,
    last_contacted_activity_time,

    CASE
        WHEN expected_deal_value IS NOT NULL THEN CAST(TRY_CAST(expected_deal_value AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS expected_deal_value,

    signing_delay_days,

    -- AM user ID masking
    CASE
        WHEN am_user_id IS NOT NULL
        THEN substr(am_user_id, 1, 2) || '***'
        ELSE NULL
    END AS am_user_id,

    -- Sales account ID masking
    CASE
        WHEN sales_account_id IS NOT NULL
        THEN regexp_replace(sales_account_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS sales_account_id,

    deal_type_id,
    deal_reason_id,
    currency_id,

    vnd_to_usd,

    deal_payment_status,
    deal_stage_name,

    sign_date,

    deal_stage_code,

    due_date,

    is_signed,

    probability_bucket,

    contract_type,
    deal_reason

FROM hive.bi_silver.crm_deals;

-- hive.bi_silver.crm_partners definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_partners
SECURITY DEFINER
AS
SELECT
    id,

    -- Partner name masking
    CASE
        WHEN partner_name IS NOT NULL
        THEN substr(partner_name, 1, 3) || '***'
        ELSE NULL
    END AS partner_name,

    -- Document number masking
    CASE
        WHEN document_number IS NOT NULL
        THEN regexp_replace(document_number, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS document_number,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    -- Address masking
    CASE
        WHEN partner_address IS NOT NULL
        THEN substr(partner_address, 1, 10) || '***'
        ELSE NULL
    END AS partner_address,

    country,

    -- Contact name masking
    CASE
        WHEN contact_name IS NOT NULL
        THEN substr(contact_name, 1, 1) || '***'
        ELSE NULL
    END AS contact_name,

    contact_position,

    -- Mobile masking
    CASE
        WHEN contact_mobile IS NOT NULL
        THEN regexp_replace(
            contact_mobile,
            '(\\d{3})\\d{4}(\\d+)',
            '$1****$2'
        )
        ELSE NULL
    END AS contact_mobile,

    -- Email masking
    CASE
        WHEN contact_email IS NOT NULL
        THEN regexp_replace(
            contact_email,
            '^(.{2}).+(@.+)$',
            '$1***$2'
        )
        ELSE NULL
    END AS contact_email,

    effective_date,
    expiration_date,

    partner_type,
    document_type,
    partner_status,

    partner_type_vi,
    document_format_vi,
    partner_status_vi

FROM hive.bi_silver.crm_partners;

-- hive.bi_silver.crm_expected_revenue definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_expected_revenue
SECURITY DEFINER
AS
SELECT
    payment_date,

    deal_id,

    update_at,

    -- Monetary masking (dev-safe): keep coarse magnitude only
    CASE
        WHEN currency_amount IS NOT NULL THEN CAST(TRY_CAST(currency_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS currency_amount,
    CASE
        WHEN vnd_amount IS NOT NULL THEN CAST(TRY_CAST(vnd_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vnd_amount,

    is_actual,

    due_date,

    product_type,
    deployment_type,
    product_category,

    is_recurring,

    territory_name,
    product_index,

    -- Customer ID masking
    CASE
        WHEN customer_id IS NOT NULL
        THEN regexp_replace(customer_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS customer_id,

    -- Customer name masking
    CASE
        WHEN customer_name IS NOT NULL
        THEN substr(customer_name, 1, 3) || '***'
        ELSE NULL
    END AS customer_name,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    customer_group,

    product_category_index,

    currency_code,

    deal_stage_name,

    -- AM username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username,

    -- Contract ID masking
    CASE
        WHEN contract_id IS NOT NULL
        THEN regexp_replace(contract_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_id,

    payment_index,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    -- Sales admin username masking
    CASE
        WHEN sales_admin_username IS NOT NULL
        THEN substr(sales_admin_username, 1, 2) || '***'
        ELSE NULL
    END AS sales_admin_username,

    -- Allocation ID masking
    CASE
        WHEN contract_allocation_id IS NOT NULL
        THEN regexp_replace(contract_allocation_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_allocation_id

FROM hive.bi_silver.crm_expected_revenue;

-- hive.bi_silver.crm_committed_revenue definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_committed_revenue
SECURITY DEFINER
AS
SELECT
    report_date,
    report_year,
    report_month,

    product_category_code,
    product_category,
    product_growth_type,
    product_service_group,

    CASE
        WHEN revenue_amount IS NOT NULL THEN CAST(TRY_CAST(revenue_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS revenue_amount,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    revenue_type,
    channel_name,

    is_soc,

    department_alias,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    revenue_group,

    CASE
        WHEN vat IS NOT NULL THEN CAST(TRY_CAST(vat AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vat,

    payment_stage,

    -- AM username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username,

    -- Presale name masking
    CASE
        WHEN presale_name IS NOT NULL
        THEN substr(presale_name, 1, 2) || '***'
        ELSE NULL
    END AS presale_name,

    currency_code,

    business_unit_level_1,

    territory_name,

    deal_id,

    update_at,

    CASE
        WHEN currency_amount IS NOT NULL THEN CAST(TRY_CAST(currency_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS currency_amount,

    is_actual,

    due_date,

    deployment_type,

    pricebook_id,

    is_recurring,

    product_index,

    -- Customer ID masking
    CASE
        WHEN customer_id IS NOT NULL
        THEN regexp_replace(customer_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS customer_id,

    -- Customer name masking
    CASE
        WHEN customer_name IS NOT NULL
        THEN substr(customer_name, 1, 3) || '***'
        ELSE NULL
    END AS customer_name,

    customer_group,

    product_category_index,

    -- Contract ID masking
    CASE
        WHEN contract_id IS NOT NULL
        THEN regexp_replace(contract_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_id,

    payment_index,

    -- Contract allocation ID masking
    CASE
        WHEN contract_allocation_id IS NOT NULL
        THEN regexp_replace(contract_allocation_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_allocation_id

FROM hive.bi_silver.crm_committed_revenue;

-- hive.bi_silver.crm_pricebook definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_pricebook
SECURITY DEFINER
AS
SELECT
    product_id,

    product_name,
    product_description,

    product_group,
    product_sub_group,

    category_code,
    product_category,

    -- SKU partial masking
    CASE
        WHEN sku IS NOT NULL
        THEN regexp_replace(sku, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS sku,

    product_version,

    item_type,

    is_active,

    product_type,
    sub_type,

    product_license,

    price_type,

    package,

    price,

    currency_code,

    price_min,
    price_max,

    price_unit,

    is_quantity_based,
    is_related_csmp,

    csmp_discount,
    csmp_discount_silver,
    csmp_discount_gold,
    csmp_discount_diamond,

    created_at,
    updated_at,

    -- Item code masking
    CASE
        WHEN item_code IS NOT NULL
        THEN regexp_replace(item_code, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS item_code,

    -- IDD masking
    CASE
        WHEN idd IS NOT NULL
        THEN regexp_replace(idd, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS idd

FROM hive.bi_silver.crm_pricebook;

-- hive.bi_silver.crm_product_category definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_product_category
SECURITY DEFINER
AS
SELECT
    category_code,
    product_category
FROM hive.bi_silver.crm_product_category;

-- hive.bi_silver.crm_product_tree definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_product_tree
SECURITY DEFINER
AS
SELECT
    product_group,
    product_sub_group,

    category_code,
    product_category,

    -- SKU partial masking
    CASE
        WHEN sku IS NOT NULL
        THEN regexp_replace(sku, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS sku,

    version,

    item_type,

    license,

    price_type,

    unit,

    -- Item code masking
    CASE
        WHEN item_code IS NOT NULL
        THEN regexp_replace(item_code, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS item_code,

    -- IDD masking
    CASE
        WHEN idd IS NOT NULL
        THEN regexp_replace(idd, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS idd

FROM hive.bi_silver.crm_product_tree;

-- hive.bi_silver.crm_product_tree_item_code definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_product_tree_item_code
SECURITY DEFINER
AS
SELECT
    idd,

    -- item_code masking (sensitive business identifier)
    CASE
        WHEN item_code IS NOT NULL
        THEN regexp_replace(item_code, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS item_code

FROM hive.bi_silver.crm_product_tree_item_code;

-- hive.bi_silver.crm_sales_accounts definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_sales_accounts
SECURITY DEFINER
AS
SELECT
    id,

    is_partner,

    -- Company/account name masking
    CASE
        WHEN name IS NOT NULL
        THEN substr(name, 1, 3) || '***'
        ELSE NULL
    END AS name,

    -- Address masking
    CASE
        WHEN address IS NOT NULL
        THEN substr(address, 1, 10) || '***'
        ELSE NULL
    END AS address,

    city,
    state,

    -- Zipcode masking
    CASE
        WHEN zipcode IS NOT NULL
        THEN regexp_replace(zipcode, '^(.{2}).*(.{1})$', '$1***$2')
        ELSE NULL
    END AS zipcode,

    country,

    number_of_employees,

    CASE
        WHEN annual_revenue IS NOT NULL THEN CAST(TRY_CAST(annual_revenue AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS annual_revenue,

    -- Website masking
    CASE
        WHEN website IS NOT NULL
        THEN regexp_replace(
            website,
            '^(https?://)?([^/]+).*$',
            '$1***'
        )
        ELSE NULL
    END AS website,

    -- AM username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username,

    -- Sales admin masking
    CASE
        WHEN sales_admin IS NOT NULL
        THEN substr(sales_admin, 1, 2) || '***'
        ELSE NULL
    END AS sales_admin,

    -- Phone masking
    CASE
        WHEN phone IS NOT NULL
        THEN regexp_replace(
            phone,
            '(\\d{3})\\d{4}(\\d+)',
            '$1****$2'
        )
        ELSE NULL
    END AS phone,

    CASE
        WHEN open_deals_amount IS NOT NULL THEN CAST(TRY_CAST(open_deals_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS open_deals_amount,
    open_deals_count,

    CASE
        WHEN won_deals_amount IS NOT NULL THEN CAST(TRY_CAST(won_deals_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS won_deals_amount,
    won_deals_count,

    -- Tax code hashing
    CASE
        WHEN tax_code IS NOT NULL
        THEN to_hex(md5(to_utf8(tax_code)))
        ELSE NULL
    END AS tax_code,

    -- Company ID masking
    CASE
        WHEN company_id IS NOT NULL
        THEN regexp_replace(company_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS company_id,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    incorporation_date,

    initial_source,
    warm_up_source,

    using_soc,
    vcs_soc_others,
    soc_brand,

    service_level,

    created_at,
    updated_at,

    days_since_update,

    -- Parent account ID masking
    CASE
        WHEN parent_sales_account_id IS NOT NULL
        THEN regexp_replace(parent_sales_account_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS parent_sales_account_id,

    -- Recent note masking
    CASE
        WHEN recent_note IS NOT NULL
        THEN '***MASKED***'
        ELSE NULL
    END AS recent_note,

    last_contacted_via_sales_activity,
    last_contacted_sales_activity_mode,

    last_assigned_at,

    renewal_date,

    business_type,
    industry_type

FROM hive.bi_silver.crm_sales_accounts;

-- hive.bi_silver.crm_users definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_users
SECURITY DEFINER
AS
SELECT
    id,

    -- Email masking
    CASE
        WHEN email IS NOT NULL
        THEN regexp_replace(
            email,
            '^(.{2}).+(@.+)$',
            '$1***$2'
        )
        ELSE NULL
    END AS email,

    -- Display name masking
    CASE
        WHEN display_name IS NOT NULL
        THEN substr(display_name, 1, 1) || '***'
        ELSE NULL
    END AS display_name,

    is_active,

    job_title

FROM hive.bi_silver.crm_users;

-- hive.bi_silver.cx_cso_support_tickets definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_cso_support_tickets
SECURITY DEFINER
AS
SELECT
    ticket_id,

    subject,

    customer_group,
    status_name,
    priority_level,

    created_at,
    updated_at,
    first_responded_at,
    resolved_at,
    closed_at,

    call_reminder,
    issue_category,

    is_vcs,
    l1,
    l2,
    l3,
    l4,

    is_duplicated_ticket,
    is_reopened_by_cx,

    -- Assigned to masking
    CASE
        WHEN assigned_to IS NOT NULL
        THEN substr(assigned_to, 1, 2) || '***'
        ELSE NULL
    END AS assigned_to,

    action_program,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    customer_satisfaction_rating,
    customer_entry_channel,

    customer_respond_date,

    communication_effectiveness,
    imported_ticket_date,
    imported_ticket_due_date,

    incident_root_cause,
    issues_type,

    -- Note masking (sensitive text)
    CASE
        WHEN note IS NOT NULL
        THEN '***MASKED***'
        ELSE NULL
    END AS note,

    number_of_due_date_changes,
    old_due_date,

    is_one_day_before_due,
    is_one_hour_before_due,

    reason,

    related_ticket,

    is_reminder_update,
    is_notification,
    is_overdue,

    response_time_minutes,
    sentiment,
    severity_level,
    spam_type,
    support_category,
    is_third_four_time,

    ttr_overdue,
    urgency_level,

    resolution_time_minutes,
    first_response_time_minutes,

    l1_time_actual_minutes,
    l1_time_allowed_minutes,
    l1_violated,

    l2_time_actual_minutes,
    l2_time_allowed_minutes,
    l2_violated,

    l3_time_actual_minutes,
    l3_time_allowed_minutes,
    l3_violated,

    l4_time_actual_minutes,
    l4_time_allowed_minutes,
    l4_violated,

    time_to_response_minutes,

    assigned_agent_stage,
    violated_level,

    -- Requester name masking
    CASE
        WHEN requester_name IS NOT NULL
        THEN substr(requester_name, 1, 2) || '***'
        ELSE NULL
    END AS requester_name,

    company_name,

    -- Agent name masking
    CASE
        WHEN agent_name IS NOT NULL
        THEN substr(agent_name, 1, 2) || '***'
        ELSE NULL
    END AS agent_name,

    resolution_time_in_business_hours,
    agent_reply_count,

    first_response_date,
    due_date_change_count,

    tickets_first_responded_within_sla,
    tickets_resolved_within_sla,

    ttr_time_minutes,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    product_category_code,
    product_category,
    product_item_type

FROM hive.bi_silver.cx_cso_support_tickets;

-- hive.bi_silver.cx_cso_ticket_tags definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_cso_ticket_tags
SECURITY DEFINER
AS
SELECT
    ticket_id,
    tag,
    created_at,
    updated_at
FROM hive.bi_silver.cx_cso_ticket_tags;

-- hive.bi_silver.cx_sur_question_answer_choices definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_sur_question_answer_choices
SECURITY DEFINER
AS
SELECT
    survey_id,
    question_id,
    question_type,

    question_clean,

    answer_choice_id,

    answer_choice_content,

    kpi_type,
    journey,
    customer_touchpoint,
    product_category

FROM hive.bi_silver.cx_sur_question_answer_choices;

-- hive.bi_silver.cx_sur_question_response definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_sur_question_response
SECURITY DEFINER
AS
SELECT
    collected_date,
    survey_id,
    response_id,

    -- respondent UUID masking
    CASE
        WHEN respondent_uuid IS NOT NULL
        THEN regexp_replace(respondent_uuid, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS respondent_uuid,

    question_id,
    question_type,

    choice_id,

    answer_string,
    answer_tag,

    -- Comment masking (free text PII risk)
    CASE
        WHEN comment IS NOT NULL
        THEN '***MASKED***'
        ELSE NULL
    END AS comment,

    choice_content,
    item_content,

    answer_number,
    item_score_value,
    item_score_label,

    question_clean,

    kpi_type,
    journey,
    customer_touchpoint,
    product_category,
    kpi_group

FROM hive.bi_silver.cx_sur_question_response;

-- hive.bi_silver.cx_sur_surveys definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.cx_sur_surveys
SECURITY DEFINER
AS
SELECT
    survey_id,
    survey_type,
    survey_name,
    folder_name,
    created_at,
    enabled,
    journey,
    customer_touchpoint,
    product_category
FROM hive.bi_silver.cx_sur_surveys;

-- hive.bi_silver.dim_customer_segment_l1 definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_customer_segment_l1
SECURITY DEFINER
AS
SELECT
    segment_l1
FROM hive.bi_silver.dim_customer_segment_l1;

-- hive.bi_silver.dim_date definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_date
SECURITY DEFINER
AS
SELECT
    date_ts,
    date_key,
    date_year,
    date_quarter,
    date_month,
    date_week,
    date_day,
    date_day_of_week,
    date_day_name,
    date_month_name,
    date_year_month,
    date_year_week,
    date_is_weekend,
    date_is_month_end,
    date_is_quarter_end
FROM hive.bi_silver.dim_date;

-- hive.bi_silver.dim_product_category definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_product_category
SECURITY DEFINER
AS
SELECT
    product_category
FROM hive.bi_silver.dim_product_category;

-- hive.bi_silver.dim_territory_name definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_territory_name
SECURITY DEFINER
AS
SELECT
    territory_name
FROM hive.bi_silver.dim_territory_name;

-- hive.bi_silver.dim_unit_level_1 definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.dim_unit_level_1
SECURITY DEFINER
AS
SELECT
    unit_level_1
FROM hive.bi_silver.dim_unit_level_1;

-- hive.bi_silver.finance_actual_cost definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_actual_cost
SECURITY DEFINER
AS
SELECT
    report_date,
    report_year,
    report_month,

    old_category,
    product_category_code,

    cost_group,

    CASE
        WHEN base_currency_amount IS NOT NULL THEN CAST(TRY_CAST(base_currency_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS base_currency_amount,

    currency_code,

    territory_name,
    product_category,

    business_unit_level_1

FROM hive.bi_silver.finance_actual_cost;

-- hive.bi_silver.finance_allocated_revenue definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_allocated_revenue
SECURITY DEFINER
AS
SELECT
    report_date,
    report_year,
    report_month,

    product_category_code,
    product_category,
    product_growth_type,
    product_service_group,

    CASE
        WHEN revenue_amount IS NOT NULL THEN CAST(TRY_CAST(revenue_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS revenue_amount,

    -- Company alias masking
    CASE
        WHEN company_alias IS NOT NULL
        THEN substr(company_alias, 1, 2) || '***'
        ELSE NULL
    END AS company_alias,

    revenue_type,
    channel_name,

    is_soc,
    department_alias,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    revenue_group,
    CASE
        WHEN vat IS NOT NULL THEN CAST(TRY_CAST(vat AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS vat,
    payment_stage,

    -- AM username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username,

    -- Presale name masking
    CASE
        WHEN presale_name IS NOT NULL
        THEN substr(presale_name, 1, 2) || '***'
        ELSE NULL
    END AS presale_name,

    currency_code,
    business_unit_level_1,
    territory_name,

    deal_id,
    update_at,

    CASE
        WHEN currency_amount IS NOT NULL THEN CAST(TRY_CAST(currency_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS currency_amount,
    is_actual,
    due_date,

    deployment_type,
    pricebook_id,
    is_recurring,
    product_index,

    -- Customer ID masking
    CASE
        WHEN customer_id IS NOT NULL
        THEN regexp_replace(customer_id, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS customer_id,

    -- Customer name masking
    CASE
        WHEN customer_name IS NOT NULL
        THEN substr(customer_name, 1, 3) || '***'
        ELSE NULL
    END AS customer_name,

    customer_group,
    product_category_index,

    -- Contract ID masking
    CASE
        WHEN contract_id IS NOT NULL
        THEN regexp_replace(contract_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_id,

    payment_index,

    -- Contract allocation masking
    CASE
        WHEN contract_allocation_id IS NOT NULL
        THEN regexp_replace(contract_allocation_id, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_allocation_id

FROM hive.bi_silver.finance_allocated_revenue;

-- hive.bi_silver.finance_contract_collected_invoices definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_contract_collected_invoices
SECURITY DEFINER
AS
SELECT
    idd,
    sales_channel,
    transaction_code,

    -- Invoice number masking
    CASE
        WHEN invoice_number IS NOT NULL
        THEN regexp_replace(invoice_number, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS invoice_number,

    -- Contract number masking
    CASE
        WHEN contract_num IS NOT NULL
        THEN regexp_replace(contract_num, '^(.{3}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS contract_num,

    -- Customer code masking
    CASE
        WHEN customer_code IS NOT NULL
        THEN regexp_replace(customer_code, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS customer_code,

    contract_description,

    -- Company name masking
    CASE
        WHEN company_name IS NOT NULL
        THEN substr(company_name, 1, 3) || '***'
        ELSE NULL
    END AS company_name,

    CASE
        WHEN original_amount IS NOT NULL THEN CAST(TRY_CAST(original_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS original_amount,
    CASE
        WHEN initial_receivable_amount IS NOT NULL THEN CAST(TRY_CAST(initial_receivable_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS initial_receivable_amount,
    CASE
        WHEN total_received_amount IS NOT NULL THEN CAST(TRY_CAST(total_received_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS total_received_amount,
    CASE
        WHEN remaining_balance_amount IS NOT NULL THEN CAST(TRY_CAST(remaining_balance_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS remaining_balance_amount,

    invoice_date,
    payment_deadline,
    payment_date,

    overdue_months_numeric,
    overdue_status_enum,

    -- AM username masking
    CASE
        WHEN am_username IS NOT NULL
        THEN substr(am_username, 1, 2) || '***'
        ELSE NULL
    END AS am_username

FROM hive.bi_silver.finance_contract_collected_invoices;

-- hive.bi_silver.finance_cost_plan definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_cost_plan
SECURITY DEFINER
AS
SELECT
    plan_date,
    plan_year,
    plan_month,

    cost_code,
    cost_group,

    CASE
        WHEN expected_cost_value IS NOT NULL THEN CAST(TRY_CAST(expected_cost_value AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS expected_cost_value,

    currency_code

FROM hive.bi_silver.finance_cost_plan;

-- hive.bi_silver.finance_fact_ratios definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_fact_ratios
SECURITY DEFINER
AS
SELECT
    report_date,
    metric_code,
    metric_name,
    metric_value
FROM hive.bi_silver.finance_fact_ratios;

-- hive.bi_silver.finance_product_revenue_plan definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_product_revenue_plan
SECURITY DEFINER
AS
SELECT
    plan_date,
    plan_year,
    plan_month,

    CASE
        WHEN plan_must_amount IS NOT NULL THEN CAST(TRY_CAST(plan_must_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS plan_must_amount,
    CASE
        WHEN plan_should_amount IS NOT NULL THEN CAST(TRY_CAST(plan_should_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS plan_should_amount,
    CASE
        WHEN plan_nice_amount IS NOT NULL THEN CAST(TRY_CAST(plan_nice_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS plan_nice_amount,

    product_group,
    product_category_code,
    product_category,
    product_name,

    business_group_name,
    sub_group_name,

    currency_code

FROM hive.bi_silver.finance_product_revenue_plan;

-- hive.bi_silver.finance_production_cost_allocations definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_production_cost_allocations
SECURITY DEFINER
AS
SELECT
    report_date,
    report_year,
    report_month,

    old_category,
    old_product_category,
    product_category_code,

    cost_group,

    CASE
        WHEN base_currency_amount IS NOT NULL THEN CAST(TRY_CAST(base_currency_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS base_currency_amount,

    currency_code,

    territory_name,

    expense_code,
    market_name

FROM hive.bi_silver.finance_production_cost_allocations;

-- hive.bi_silver.finance_revenue_plan definition
CREATE OR REPLACE VIEW hive.bi_silver_dev.finance_revenue_plan
SECURITY DEFINER
AS
SELECT
    plan_date,
    plan_year,
    plan_month,

    CASE
        WHEN plan_viettel_group_amount IS NOT NULL THEN CAST(TRY_CAST(plan_viettel_group_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS plan_viettel_group_amount,
    CASE
        WHEN plan_must_amount IS NOT NULL THEN CAST(TRY_CAST(plan_must_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS plan_must_amount,
    CASE
        WHEN plan_nice_amount IS NOT NULL THEN CAST(TRY_CAST(plan_nice_amount AS DOUBLE) / 1000 AS BIGINT)
        ELSE NULL
    END AS plan_nice_amount,

    segment_l1_alias,

    currency_code,

    segment_l1,
    customer_segment_l1,
    segment_l2,
    customer_segment_l2

FROM hive.bi_silver.finance_revenue_plan;

-- hive.bi_silver.giang_test_masterplan definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.giang_test_masterplan
SECURITY DEFINER
AS
SELECT
    plan_date,

    category_code,
    product_category,

    customer_segment_l1,
    customer_segment_l2,

    plan_viettel_group,
    plan_must,
    plan_should,
    plan_nice

FROM hive.bi_silver.giang_test_masterplan;

-- hive.bi_silver.hr_employee_headcount definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_employee_headcount
SECURITY DEFINER
AS
SELECT
    headcount_plan,
    headcount_status,

    division_n,
    unit_n_1,
    department_n_2,
    department_n_3,

    role_base,
    specialization,
    required_level,
    position_status,

    -- Employee name masking (PII)
    CASE
        WHEN full_name IS NOT NULL
        THEN substr(full_name, 1, 1) || '***'
        ELSE NULL
    END AS full_name,

    -- Employee code masking
    CASE
        WHEN employee_code IS NOT NULL
        THEN regexp_replace(employee_code, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS employee_code,

    employee_object,
    contract_type,
    hire_date_vcs,

    level,
    region,
    management_level,
    branch,

    -- Business email masking
    CASE
        WHEN business_email IS NOT NULL
        THEN regexp_replace(business_email, '^(.{2}).+(@.+)$', '$1***$2')
        ELSE NULL
    END AS business_email,

    job_framework_position,
    job_framework_position_2,

    service_group,
    service_group_new,

    is_ai_group,
    is_ai_org,
    is_philippines_japan_market,
    is_domestic_business,
    is_international_business,
    is_rnd,
    is_indirect_group,
    is_business_support

FROM hive.bi_silver.hr_employee_headcount;

-- hive.bi_silver.hr_employee_onboard definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_employee_onboard
SECURITY DEFINER
AS
SELECT
    -- Employee code masking
    CASE
        WHEN employee_code IS NOT NULL
        THEN regexp_replace(employee_code, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS employee_code,

    -- Full name masking
    CASE
        WHEN full_name IS NOT NULL
        THEN substr(full_name, 1, 1) || '***'
        ELSE NULL
    END AS full_name,

    current_status,
    recruitment_status,
    employee_type,

    contract_company_independent,

    branch,
    division,
    unit_level_1,
    business_unit_level_1,
    department_level_2,

    role_base,
    job_level,
    technical_grade_band,
    technical_grade_level,
    management_level,

    hire_date,
    hire_date_viettel,
    termination_date,

    -- Business email masking
    CASE
        WHEN business_email IS NOT NULL
        THEN regexp_replace(business_email, '^(.{2}).+(@.+)$', '$1***$2')
        ELSE NULL
    END AS business_email,

    email_nametag,

    date_of_birth,
    gender,

    pr_q1_2024,
    pr_q2_2024,
    pr_q3_2024,
    pr_q4_2024,
    pr_q1_2025,
    pr_q2_2025,
    pr_q3_2025,

    achievement_title_2024,
    talent_group,
    performance_level,
    potential_level,

    termination_status,
    termination_reason_1,
    termination_reason_2,
    termination_detail_reason,

    next_workplace,

    is_new_hire,

    age,
    age_group,

    department_level_3,

    key_core_group,
    is_key_employee,
    employee_criticality_level

FROM hive.bi_silver.hr_employee_onboard;

-- hive.bi_silver.hr_employee_resigned definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.hr_employee_resigned
SECURITY DEFINER
AS
SELECT
    -- Employee code masking
    CASE
        WHEN employee_code IS NOT NULL
        THEN regexp_replace(employee_code, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END AS employee_code,

    -- Full name masking
    CASE
        WHEN full_name IS NOT NULL
        THEN substr(full_name, 1, 1) || '***'
        ELSE NULL
    END AS full_name,

    current_status,

    -- Business email masking
    CASE
        WHEN business_email IS NOT NULL
        THEN regexp_replace(business_email, '^(.{2}).+(@.+)$', '$1***$2')
        ELSE NULL
    END AS business_email,

    email_nametag,

    recruitment_status,
    employee_type,
    contract_company_independent,

    branch,
    division,
    unit_level_1,
    business_unit_level_1,

    department_level_2,
    department_n_3,

    role_base,
    job_level,

    region,
    management_level,

    hire_date,
    hire_date_vcs,
    termination_date,

    date_of_birth,
    gender,

    seniority_group,

    pr_q1_2024,
    pr_q2_2024,
    pr_q3_2024,
    pr_q4_2024,
    pr_q1_2025,
    pr_q2_2025,

    achievement_2024,
    talent_9box_group,
    performance_level,
    potential_level,

    leave_status,

    resignation_reason_level_1,
    resignation_reason_level_2,
    resignation_reason_detail,

    next_workplace_score,

    employment_status,
    termination_reason,

    is_key_employee,
    is_high_potential

FROM hive.bi_silver.hr_employee_resigned;

-- hive.bi_silver.jira_msn definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.jira_msn
SECURITY DEFINER
AS
SELECT
    key,
    start_date,
    summary,
    issuetype,
    status,

    -- Assignee masking (PII)
    CASE
        WHEN assignee IS NOT NULL
        THEN substr(assignee, 1, 2) || '***'
        ELSE NULL
    END AS assignee,

    projectkey,
    department_center,
    task_type,

    duedate,
    created,
    updated

FROM hive.bi_silver.jira_msn;

-- hive.bi_silver.jira_task_operation definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.jira_task_operation
SECURITY DEFINER
AS
SELECT
    key,
    start_date,
    due_date,
    summary,
    issue_type,
    priority,
    status,

    -- Assignor masking
    CASE
        WHEN assignor IS NOT NULL
        THEN substr(assignor, 1, 2) || '***'
        ELSE NULL
    END AS assignor,

    work_group,
    department_center,

    -- Assignee masking
    CASE
        WHEN assignee IS NOT NULL
        THEN substr(assignee, 1, 2) || '***'
        ELSE NULL
    END AS assignee,

    created,
    updated

FROM hive.bi_silver.jira_task_operation;

-- hive.bi_silver.noc_entities definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.noc_entities
SECURITY DEFINER
AS
SELECT
    customer,
    project,

    -- Host masking (sensitive infrastructure info)
    CASE
        WHEN host IS NOT NULL
        THEN regexp_replace(host, '^(.{3}).*(.{3})$', '$1****$2')
        ELSE NULL
    END AS host

FROM hive.bi_silver.noc_entities;

-- hive.bi_silver.noc_metrics definition

CREATE OR REPLACE VIEW hive.bi_silver_dev.noc_metrics
SECURITY DEFINER
AS
SELECT
    metric_name,
    agg_type,
    customer,
    project,

    -- Host masking (infrastructure-sensitive)
    CASE
        WHEN host IS NOT NULL
        THEN regexp_replace(host, '^(.{3}).*(.{3})$', '$1****$2')
        ELSE NULL
    END AS host,

    metric_value,
    metric_ts,
    metric_time,
    dt

FROM hive.bi_silver.noc_metrics;

-- hive.bi_silver.crm_company_contacts source

CREATE OR REPLACE VIEW hive.bi_silver_dev.crm_company_contacts
SECURITY DEFINER
AS
SELECT
    company_id,

    -- Company name: chỉ format, không expose raw full text nếu coi là sensitive business identifier
    CASE
        WHEN company_name IS NOT NULL
        THEN substr(company_name, 1, 3) || '***'
        ELSE NULL
    END AS company_name,

    created_at,
    updated_at

FROM hive.bi_silver.company_contacts;

-- hive.bi_silver.crm_deal_interested_product_stats source

CREATE VIEW hive.bi_silver_dev.crm_deal_interested_product_stats SECURITY DEFINER AS
SELECT
    interested_product_category product_category
, count(*) number_of_deals
FROM
  bi_silver.crm_deal_interested_products
GROUP BY 1;

-- hive.bi_silver.finance_daily_business_metrics source

CREATE VIEW hive.bi_silver_dev.finance_daily_business_metrics SECURITY DEFINER AS
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
     hive.bi_silver.crm_committed_revenue
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
     hive.bi_silver.crm_committed_revenue
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
, CAST(SUM(actual_cost_amount) / 1000 AS BIGINT) actual_cost_amount
, CAST(SUM(allocated_revenue_amount) / 1000 AS BIGINT) allocated_revenue_amount
, CAST(SUM(allocated_revenue_amount) / 1000 AS BIGINT) cash_collection_amount
, CAST(SUM(sales_revenue_amount) / 1000 AS BIGINT) sales_revenue_amount
, CAST(SUM(planned_revenue_must_amount) / 1000 AS BIGINT) planned_revenue_must_amount
, CAST(SUM(planned_revenue_nice_amount) / 1000 AS BIGINT) planned_revenue_nice_amount
, CAST((SUM(sales_revenue_amount) - SUM(actual_cost_amount)) / 1000 AS BIGINT) gross_profit_amount
, CAST((SUM(allocated_revenue_amount) - SUM(planned_revenue_must_amount)) / 1000 AS BIGINT) revenue_vs_plan_gap_amount
FROM
  fact_union
GROUP BY 1, 2, 3, 4;

-- hive.bi_silver.finance_metrics source

CREATE VIEW hive.bi_silver_dev.finance_metrics SECURITY DEFINER AS
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
     hive.bi_silver.crm_committed_revenue
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
, CAST(TRY_CAST(total_revenue AS DOUBLE) / 1000 AS BIGINT) total_revenue
, CAST(TRY_CAST(total_cost AS DOUBLE) / 1000 AS BIGINT) total_cost
, CAST((total_revenue - (direct_labor_os_cost + production_infra_cost)) / 1000 AS BIGINT) gross_profit
, CAST(((total_revenue - total_cost) + fixed_asset_depreciation_cost) / 1000 AS BIGINT) ebitda
, CAST(TRY_CAST(fixed_asset_depreciation_cost AS DOUBLE) / 1000 AS BIGINT) fixed_asset_depreciation_cost
, CAST(TRY_CAST(direct_labor_os_cost AS DOUBLE) / 1000 AS BIGINT) direct_labor_os_cost
, CAST(TRY_CAST(production_infra_cost AS DOUBLE) / 1000 AS BIGINT) production_infra_cost
, CAST(TRY_CAST(business_os_cost AS DOUBLE) / 1000 AS BIGINT) business_os_cost
FROM
  final_join;

-- hive.bi_silver.hr_master_report source

CREATE VIEW hive.bi_silver_dev.hr_master_report SECURITY DEFINER AS
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
    CASE
        WHEN employee_code IS NOT NULL THEN regexp_replace(employee_code, '^(.{2}).*(.{2})$', '$1****$2')
        ELSE NULL
    END employee_code
, CASE
        WHEN email_nametag IS NOT NULL THEN substr(email_nametag, 1, 2) || '***'
        ELSE NULL
    END email_username
, current_status current_status
, division division
, business_unit_level_1 business_unit_level_1
, department_level_2 department_level_2
, job_level job_level
, management_level management_level
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

-- hive.bi_silver.cx_cso_customer_ticket_monthly source

CREATE VIEW hive.bi_silver_dev.cx_cso_customer_ticket_monthly SECURITY DEFINER AS
SELECT
    product_category product_category
, company_name company_name
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
