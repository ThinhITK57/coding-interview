# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.task_operation")
 
sql_query = """

WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY key
                ORDER BY updated_ts DESC
            ) AS rn
        FROM jira_raw.task_operation
    ) t
    WHERE rn = 1
)

SELECT
    -- ========= IDENTIFIER =========
    key AS issue_key,

    -- ========= ISSUE INFO =========
    summary     AS summary,
    issuetype   AS issue_type,
    status      AS status,
    priority    AS priority,

    -- ========= ASSIGNMENT =========
    assignee    AS assignee,
    assignor    AS assignor,
    work_group AS work_group,
    department_center AS department_center,

    -- ========= RAW STRING TIME =========
--    created     AS created_str,
--    updated     AS updated_str,
--    start_date AS start_date_str,
--    duedate     AS due_date_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    CAST(created    AS TIMESTAMP) AS created_time,
    CAST(updated    AS TIMESTAMP) AS updated_time,
    CAST(start_date AS TIMESTAMP) AS start_date_time,
    CAST(duedate    AS TIMESTAMP) AS due_date_time

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(created_ts    / 1000) AS TIMESTAMP) AS created_time_ts,
--    TRY_CAST(from_unixtime(updated_ts    / 1000) AS TIMESTAMP) AS updated_time_ts,
--    TRY_CAST(from_unixtime(start_date_ts / 1000) AS TIMESTAMP) AS start_date_time_ts,
--    TRY_CAST(from_unixtime(duedate_ts    / 1000) AS TIMESTAMP) AS due_date_time_ts,

    -- ========= RAW TS =========
--    created_ts,
--    updated_ts,
--    start_date_ts,
--    duedate_ts,

    -- ========= META =========
--    source_file
FROM last_items_cte


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/jira-silver/task_operation"
    ) \
    .saveAsTable("jira_silver.task_operation")
