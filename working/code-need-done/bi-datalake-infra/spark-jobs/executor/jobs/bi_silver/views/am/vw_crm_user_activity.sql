CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_user_activity AS
SELECT
    CAST(user_id AS varchar) AS am_user_id,

    display_name AS am_user_name,
    email,

    is_active,
    job_title,
    role_name,
    team_name,
    territory_name ,

    last_login_at,

    CASE
        WHEN last_login_at IS NOT NULL THEN 1
        ELSE 0
    END AS has_login,

    CASE
        WHEN last_login_at >= current_timestamp - INTERVAL '1' DAY
        THEN 1
        ELSE 0
    END AS login_last_1d,

    CASE
        WHEN last_login_at >= current_timestamp - INTERVAL '7' DAY
        THEN 1
        ELSE 0
    END AS login_last_7d,

    CASE
        WHEN last_login_at >= current_timestamp - INTERVAL '30' DAY
        THEN 1
        ELSE 0
    END AS login_last_30d

FROM hive.bi_silver.crm_users
WHERE role_name  = 'AM';