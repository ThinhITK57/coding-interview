CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_contact_activity AS
SELECT
    CAST(id AS varchar) AS object_id,
    'CONTACT' AS object_type,

    user_am_id AS am_user_id,
    user_am_name AS am_user_name,

    CAST(created_at AS date) AS activity_date,

    created_at,
    updated_at,
    last_contacted_at,
    last_contacted_via_sales_activity,
    last_assigned_at,

    CASE
        WHEN created_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_created,

    CASE
        WHEN updated_at IS NOT NULL
         AND updated_at > created_at
        THEN 1
        ELSE 0
    END AS is_updated,

    CASE
        WHEN last_contacted_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_contacted,

    contact_status_id,
    contact_status_name,
    contact_forecast_type,

    company_name,
    sales_account_id,
    sales_account_name,
    center_name

FROM hive.bi_silver.crm_contacts
Where user_am_id in  (select CAST(user_id AS varchar) FROM hive.bi_silver.crm_users u_owner WHERE u_owner.role_name  = 'AM' )
;