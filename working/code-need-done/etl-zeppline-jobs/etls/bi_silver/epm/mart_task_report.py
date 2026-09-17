# %livy.pyspark

tgt_table = "bi_silver.epm_mart_task_report"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_mart_task_report"

spark.sql("REFRESH TABLE bi_silver.fact_epm_task_execution_snapshot")
spark.sql("REFRESH TABLE bi_silver.dim_epm_department")
spark.sql("REFRESH TABLE bi_silver.dim_epm_resource")
spark.sql("REFRESH TABLE bi_silver.dim_epm_project")

sql_query = """
WITH latest_fact AS (
    SELECT *
    FROM bi_silver.fact_epm_task_execution_snapshot
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM bi_silver.fact_epm_task_execution_snapshot
    )
)
SELECT
    f.task_type,

    f.assignee,
    COALESCE(d_res.resource_name, 'Unknown')    AS assignee_name,

    f.department,
    COALESCE(d_dept.department_name, 'Unknown') AS department_name,

    f.resources,

    f.parent_project,
    COALESCE(d_proj.project_name, 'Unknown')    AS project_name,

    f.jira_status,
    f.name,
    f.description,
    f.work,
    f.duration,
    f.epm_default,
    f.start_date,
    f.due_date,
    f.percent_completed,
    f.update_description

FROM latest_fact f

-- JOIN chuẩn hóa với dim_department, dim_resource, dim_project
LEFT JOIN bi_silver.dim_epm_department d_dept
    ON f.department_key = d_dept.department_key

LEFT JOIN bi_silver.dim_epm_resource d_res
    ON f.assignee_key = d_res.resource_key

LEFT JOIN bi_silver.dim_epm_project d_proj
    ON f.project_key = d_proj.project_key
"""

df = spark.sql(sql_query)

df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table}")