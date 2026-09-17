# %livy.pyspark

tgt_table = "bi_silver.jira_msn"
tgt_path = "s3a://bi-silver/jira_msn"

sql_query = """
    SELECT DISTINCT
        CAST(`key` AS STRING)               AS `key`,
        TO_TIMESTAMP(start_date)            AS start_date,
        summary                             AS summary,
        issuetype                           AS issuetype,
        status                              AS status,
        assignee                            AS assignee,
        projectkey                          AS projectkey,
        projectname                         AS department_center,
        task_type                           AS task_type,
        TO_TIMESTAMP(duedate)               AS duedate,
        TO_TIMESTAMP(created)               AS created,
        TO_TIMESTAMP(updated)               AS updated
    FROM jira_raw.msn_task
"""


df = spark.sql(sql_query)

# # DROP TABLE + hard delete path để chống double file
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)


# Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())