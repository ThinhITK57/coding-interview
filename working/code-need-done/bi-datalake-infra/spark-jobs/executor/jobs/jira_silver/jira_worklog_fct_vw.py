# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.jira_worklog_fct_vw")
 
sql_query = """

-- CREATE VIEW jira_silver.jira_worklog_fct_vw AS
SELECT
  i.issue_key,
  i.project_key,
  i.assignee,
  w.author,
  w.timespent_hours,
  w.started_time as work_start_time
FROM jira_silver.jira_issue_task_fct_vw i
LEFT JOIN jira_silver.worklog w
  ON i.issue_key = w.issue_key

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/jira-silver/jira_worklog_fct_vw"
    ) \
    .saveAsTable("jira_silver.jira_worklog_fct_vw")
