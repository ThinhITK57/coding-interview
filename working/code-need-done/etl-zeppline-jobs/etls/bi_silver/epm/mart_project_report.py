%livy.pyspark

tgt_table = "bi_gold.project_report"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_gold/data/project_report"

spark.sql("REFRESH TABLE bi_silver.fact_epm_project_progress_snapshot")
spark.sql("REFRESH TABLE bi_silver.dim_epm_department")
spark.sql("REFRESH TABLE bi_silver.dim_epm_resource")

sql_query = """
WITH latest_fact AS (
    SELECT *
    FROM bi_silver.fact_epm_project_progress_snapshot
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM bi_silver.fact_epm_project_progress_snapshot
    )
)
SELECT
    f.name,
    f.project_type,
    f.due_date,
    f.state,
    f.status,
    f.percent_completed,

    f.department,
    COALESCE(d_dept.department_name, 'Unknown')   AS department_name,

    f.assignor,
    COALESCE(d_assignor.resource_name, 'Unknown') AS assignor_name,

    f.assignee,
    COALESCE(d_assignee.resource_name, 'Unknown') AS assignee_name,

    f.project_manager,
    COALESCE(d_pm.resource_name, 'Unknown')       AS project_manager_name,

    f.resources

FROM latest_fact f

-- JOIN với Dims
LEFT JOIN bi_silver.dim_epm_department d_dept
    ON f.department_key = d_dept.department_key

LEFT JOIN bi_silver.dim_epm_resource d_assignee
    ON f.assignee_key = d_assignee.resource_key

LEFT JOIN bi_silver.dim_epm_resource d_assignor
    ON f.assignor_key = d_assignor.resource_key

LEFT JOIN bi_silver.dim_epm_resource d_pm
    ON f.pm_key = d_pm.resource_key
"""

df = spark.sql(sql_query)

df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

# Tạo alias tương thích ngược
spark.sql(f"CREATE OR REPLACE VIEW bi_silver.epm_mart_project_report AS SELECT * FROM {tgt_table}")

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table}")