# %livy.pyspark
spark.sql("create  database IF NOT EXISTS jira_silver")
spark.sql("use jira_silver")