CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_quotation_activity AS
SELECT
    quotation_id AS object_id,
    'QUOTATION' AS object_type,

    d.am_user_id,
    d.am_user_name,

    CAST(q.created_at AS date) AS activity_date,

    q.created_at,

    CASE
        WHEN q.created_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_created,

    q.status,
    q.currency_code,
    q.global_discount,
    q.global_discount_type,

    q.deal_id,

    d.deal_name,
    d.company_id,
    d.company_name

FROM hive.bi_silver.crm_deal_quotations q

LEFT JOIN hive.bi_silver.crm_deals d
    ON q.deal_id = d.deal_id
    
Where d.am_user_id in  (select CAST(user_id AS varchar) FROM hive.bi_silver.crm_users u_owner WHERE u_owner.role_name  = 'AM' );