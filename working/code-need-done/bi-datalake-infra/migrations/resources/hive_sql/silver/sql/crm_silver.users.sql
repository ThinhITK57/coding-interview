WITH last_st_users AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.crm_raw.st_users su
    ) t
    WHERE rn = 1
),
last_roles AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.crm_raw.roles r
    ) t
    WHERE rn = 1
)

SELECT
    CAST(su.id AS VARCHAR)      AS am_user_id,
    su.display_name             AS display_name,
    su.email                    AS email,
    su.is_active                AS is_active,
    su.work_number              AS work_number,
    su.mobile_number            AS mobile_number,
    su.confirmed                AS confirmed,
    su.job_title                AS job_title,
    su.language                 AS language,
    su.last_login_at            AS last_login_at,
    su.time_zone                AS time_zone,
    su.access_scope             AS access_scope,
    su.reports_to_id            AS line_manager_id,
    su.role_id                  AS role_id,
    lr.name                     AS role_name,
    su.user_access_type         AS user_access_type
FROM last_st_users su
LEFT JOIN last_roles lr
    ON CAST(lr.id AS VARCHAR) = su.role_id;
