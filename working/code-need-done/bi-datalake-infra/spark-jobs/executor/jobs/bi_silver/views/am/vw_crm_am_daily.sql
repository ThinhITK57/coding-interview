CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_am_daily AS
SELECT
    activity_date,
    am_user_id,
    am_user_name,

    COUNT(DISTINCT CASE
        WHEN is_created = 1 THEN object_id
    END) AS created_object_count,

    COUNT(DISTINCT CASE
        WHEN is_updated = 1 THEN object_id
    END) AS updated_object_count,

    COUNT(DISTINCT CASE
        WHEN is_progressed = 1 THEN object_id
    END) AS progressed_object_count,

    COUNT(DISTINCT CASE
        WHEN is_contacted = 1 THEN object_id
    END) AS contacted_object_count,

    COUNT(DISTINCT CASE
        WHEN is_signed = 1 THEN object_id
    END) AS signed_object_count,

    COUNT(DISTINCT CASE
        WHEN is_business_outcome = 1 THEN object_id
    END) AS business_outcome_count

FROM hive.bi_silver.vw_crm_am_object_activity
GROUP BY
    activity_date,
    am_user_id,
    am_user_name;