%livy.pyspark
# spark.sql("DROP TABLE IF EXISTS crm_silver.st_users211")
spark.sql("REFRESH TABLE crm_raw.roles")
spark.sql("REFRESH TABLE crm_raw.st_users")

sql_query = """
WITH last_st_users AS (
    SELECT *
    FROM (
        SELECT
            su.*,
            ROW_NUMBER() OVER (
                PARTITION BY su.id
                ORDER BY su.updated_at_ts DESC, su.id DESC
            ) AS rn
        FROM crm_raw.st_users su
    ) t
    WHERE rn = 1
),
last_roles AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.id
                ORDER BY r.updated_at_ts DESC, r.id DESC
            ) AS rn
        FROM crm_raw.roles r
    ) t
    WHERE rn = 1
)

SELECT
    CAST(su.id AS STRING)        AS am_user_id,
    su.display_name              AS display_name,
    su.email                     AS email,
    su.is_active                 AS is_active,
    su.work_number               AS work_number,
    su.mobile_number             AS mobile_number,
    su.confirmed                 AS confirmed,
    su.job_title                 AS job_title,
    su.language                  AS language,
    su.last_login_at             AS last_login_at,
    su.time_zone                 AS time_zone,
    su.access_scope              AS access_scope,
    su.reports_to_id             AS line_manager_id,
    su.role_id                   AS role_id,
    lr.name                      AS role_name,
    su.user_access_type          AS user_access_type
FROM last_st_users su
LEFT JOIN last_roles lr
    ON CAST(lr.id AS STRING) = su.role_id
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "/opt/datasets/crawlers/vcs_silver/crm_silver/data/users"
    ) \
    .saveAsTable("crm_silver.users")
