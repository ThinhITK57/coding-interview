CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_deal_activity AS
SELECT
    deal_id AS object_id,
    'OPPORTUNITY' AS object_type,

    am_user_id,
    am_user_name,

    CAST(created_at AS date) AS activity_date,

    created_at,
    updated_at,
    stage_updated_time,
    expected_close_date,
    closed_date,
    sign_date,

    deal_stage_id,
    deal_stage_name,
    deal_stage_code,
    deal_status,

    CASE
        WHEN created_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_created,

    CASE
        WHEN updated_at > created_at THEN 1
        ELSE 0
    END AS is_updated,

    CASE
        WHEN stage_updated_time IS NOT NULL THEN 1
        ELSE 0
    END AS is_stage_progressed,

    CASE
        WHEN closed_date IS NOT NULL THEN 1
        ELSE 0
    END AS is_closed,

    CASE
        WHEN sign_date IS NOT NULL THEN 1
        ELSE 0
    END AS is_signed,

    CASE
        WHEN has_quotations THEN 1
        ELSE 0
    END AS has_quotation,

    CASE
        WHEN has_allocated_products THEN 1
        ELSE 0
    END AS has_products,

    CASE
        WHEN has_allocated_records THEN 1
        ELSE 0
    END AS has_allocated_records,

    company_id,
    company_name,
    company_tax_code,

    expected_deal_value,
    vnd_amount,

    contract_id,
    is_created_contract

FROM hive.bi_silver.crm_deals
Where am_user_id in  (select CAST(user_id AS varchar) FROM hive.bi_silver.crm_users u_owner WHERE u_owner.role_name  = 'AM' );