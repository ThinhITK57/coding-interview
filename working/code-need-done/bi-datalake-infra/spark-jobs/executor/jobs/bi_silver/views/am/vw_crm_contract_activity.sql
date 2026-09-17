CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_contract_activity AS
SELECT
    id AS object_id,
    'CONTRACT' AS object_type,

    user_am_id AS am_user_id,
    user_am_name AS am_user_name,

    CAST(created_at AS date) AS activity_date,

    created_at,
    updated_at,
    sign_date,
    fac_date,

    CASE
        WHEN created_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_created,

    CASE
        WHEN updated_at > created_at THEN 1
        ELSE 0
    END AS is_updated,

    CASE
        WHEN sign_date IS NOT NULL THEN 1
        ELSE 0
    END AS is_signed,

    CASE
        WHEN is_first_contract = 1 THEN 1
        ELSE 0
    END AS is_first_contract,

    contract_amount,
    vcs_contract_amount,
    viettel_contract_amount,

    status,
    status_code,
    contract_type,

    deal_id,
    tax_code,
    customer_name,
    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3

FROM hive.bi_silver.crm_contracts
Where user_am_id in  (select CAST(user_id AS varchar) FROM hive.bi_silver.crm_users u_owner WHERE u_owner.role_name  = 'AM' )
;