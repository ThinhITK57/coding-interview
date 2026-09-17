CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_sales_account_activity AS
SELECT
    id AS object_id,
    'SALES_ACCOUNT' AS object_type,

    CAST(am_user_id AS varchar) AS am_user_id,
    user_am_name AS am_user_name,

    CAST(created_at AS date) AS activity_date,

    created_at,
    updated_at,

    CASE
        WHEN created_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_created,

    CASE
        WHEN updated_at > created_at THEN 1
        ELSE 0
    END AS is_updated,

    CASE
        WHEN last_contacted_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_contacted,

    days_since_update,

    last_contacted_at,
    last_contacted_via_sales_activity,
    last_contacted_mode,

    name,
    company_id,
    company_alias,
    tax_code,

    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,

    open_deals_count,
    open_deals_amount,
    won_deals_count,
    won_deals_amount

FROM hive.bi_silver.crm_sales_accounts
Where am_user_id in  (select user_id FROM hive.bi_silver.crm_users u_owner WHERE u_owner.role_name  = 'AM' );