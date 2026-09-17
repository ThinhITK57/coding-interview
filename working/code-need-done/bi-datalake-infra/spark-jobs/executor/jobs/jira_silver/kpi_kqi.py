# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.kpi_kqi")
 
sql_query = """

WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY key
                ORDER BY updated_ts desc, key desc 
            ) AS rn
        FROM jira_raw.kpi_kqi
    ) t
    WHERE rn = 1
)

SELECT
    -- ========= IDENTIFIER =========
    key AS issue_key,

    -- ========= KPI INFO =========
    summary                       AS summary,
    service_product               AS service_product,
    condition                     AS condition,
    target                        AS target,

    -- ========= KPI METRICS =========
    quarterly_kpi_result          AS quarterly_kpi_result,
    monthly_accumulated_result    AS monthly_accumulated_result,
    execution_result              AS execution_result,
    unit_of_measure               AS unit_of_measure,

    -- ========= RAW STRING TIME =========
--    reporting_date                AS reporting_date_str,
--    updated                       AS updated_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    CAST(reporting_date AS TIMESTAMP) AS reporting_date_time,
    CAST(updated        AS TIMESTAMP) AS updated_time

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(reporting_date_ts / 1000) AS TIMESTAMP) AS reporting_date_time_ts,
--    TRY_CAST(from_unixtime(updated_ts        / 1000) AS TIMESTAMP) AS updated_time_ts,

    -- ========= RAW TS =========
--    reporting_date_ts,
--    updated_ts,

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
        "s3a://vcs-silver/jira-silver/kpi_kqi"
    ) \
    .saveAsTable("jira_silver.kpi_kqi")
