# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.worklog")
 
sql_query = """

WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY worklogid
                ORDER BY updated desc, worklogid desc 
            ) AS rn
        FROM jira_raw.log_work
    ) t
    WHERE rn = 1
)

SELECT
    -- ========= IDENTIFIER =========
    worklogid   AS worklog_id,
    issueid     AS issue_id,
    issuekey    AS issue_key,

    -- ========= PROJECT =========
    projectkey AS project_key,

    -- ========= ISSUE / WORKLOG INFO =========
    summary        AS summary,
    author         AS author,
    comment        AS comment,
    timespent_hours AS timespent_hours,

    -- ========= RAW STRING TIME =========
--    started AS started_str,
--    created AS created_str,
--    updated AS updated_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    CAST(started AS TIMESTAMP) AS started_time,
    CAST(created AS TIMESTAMP) AS created_time,
    CAST(updated AS TIMESTAMP) AS updated_time

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
        "s3a://vcs-silver/jira-silver/worklog"
    ) \
    .saveAsTable("jira_silver.worklog")
