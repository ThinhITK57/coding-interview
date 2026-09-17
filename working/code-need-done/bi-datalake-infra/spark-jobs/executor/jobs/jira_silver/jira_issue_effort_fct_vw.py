# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.jira_issue_effort_fct_vw")
 
sql_query = """
 -- CREATE view jira_silver.jira_issue_effort_fct_vw AS
 SELECT
    i.issue_key,
    i.project_key,
    i.status,
    u.display_name,
    f.total_net_effort,
    f.resolved_time
FROM jira_silver.jira_issue_task_fct_vw i
LEFT JOIN jira_silver.estimated_effort f
    ON i.issue_key = f.issue_key
LEFT JOIN jira_silver.jira_user u
    ON i.assignee = u.username
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/jira-silver/jira_issue_effort_fct_vw"
    ) \
    .saveAsTable("jira_silver.jira_issue_effort_fct_vw")
