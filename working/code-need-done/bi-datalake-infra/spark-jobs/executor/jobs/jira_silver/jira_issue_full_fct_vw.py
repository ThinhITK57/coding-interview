# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.jira_issue_full_fct_vw")
 
sql_query = """
-- CREATE OR REPLACE VIEW jira_silver.jira_issue_full_fct_vw AS
SELECT
    -- ========= IDENTIFIER =========
    i.issue_key,

    -- ========= PROJECT =========
    i.project_key,
    i.project_name,

    -- ========= ISSUE INFO =========
    i.issue_type,
    i.status,
    i.priority,
    i.assignee,
    u.display_name        AS assignee_name,

    -- ========= TIME =========
    i.created_time  AS created_time,
    i.updated_time  AS updated_time,
    i.resolved_time AS resolved_time,

    -- ========= EFFORT (ULNL) =========
    f.total_net_effort,

    -- ========= KPI =========
    k.quarterly_kpi_result,
    k.monthly_accumulated_result,
    k.execution_result,
    k.target,
    k.unit_of_measure,
    k.reporting_date_time AS kpi_reporting_date
FROM jira_silver.jira_issue_task_fct_vw i

-- ---- Effort
LEFT JOIN jira_silver.estimated_effort f
    ON i.issue_key = f.issue_key

-- ---- KPI
LEFT JOIN jira_silver.kpi_kqi k
    ON i.issue_key = k.issue_key

-- ---- User
LEFT JOIN jira_silver.jira_user u
    ON i.assignee = u.username

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/jira-silver/jira_issue_full_fct_vw"
    ) \
    .saveAsTable("jira_silver.jira_issue_full_fct_vw")
