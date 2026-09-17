# %livy.pyspark

tgt_table = "bi_silver.epm_mart_bsc_yearly"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_mart_bsc_yearly"

spark.sql("REFRESH TABLE bi_silver.fact_epm_target_bsc_snapshot")
spark.sql("REFRESH TABLE bi_silver.dim_epm_department")
spark.sql("REFRESH TABLE bi_silver.dim_epm_resource")
spark.sql("REFRESH TABLE bi_silver.dim_epm_objective")
spark.sql("REFRESH TABLE bi_silver.dim_epm_project")

sql_query = """
WITH latest_fact AS (
    SELECT *
    FROM bi_silver.fact_epm_target_bsc_snapshot
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM bi_silver.fact_epm_target_bsc_snapshot
    )
)
SELECT
    -- 1. Khóa ID gốc (Bảo toàn Foreign Key cho dbt tests & quan hệ dữ liệu)
    f.associated_objective,
    COALESCE(d_obj.objective_name, 'Unknown')       AS objective_name,

    f.associated_item,
    COALESCE(d_proj.project_name, 'Unknown')         AS associated_item_name,

    f.target_type,
    f.parent_target,

    f.c_department,
    COALESCE(d_dept.department_name, 'Unknown')     AS department_name,

    f.name,

    f.c_assignee,
    COALESCE(d_assignee.resource_name, 'Unknown')   AS assignee_name,

    f.unit,
    f.target_date_m,
    f.target_value_m,
    f.target_date_n,
    f.target_value_n,
    f.state,
    f.status,
    f.target_result_m,
    f.target_result_n,

    f.assignor,
    COALESCE(d_assignor.resource_name, 'Unknown')   AS assignor_name

FROM latest_fact f

-- JOIN với các Dimension đã chuẩn hóa
LEFT JOIN bi_silver.dim_epm_department d_dept
    ON f.department_key = d_dept.department_key

LEFT JOIN bi_silver.dim_epm_resource d_assignee
    ON f.assignee_key = d_assignee.resource_key

LEFT JOIN bi_silver.dim_epm_resource d_assignor
    ON f.assignor_key = d_assignor.resource_key

LEFT JOIN bi_silver.dim_epm_objective d_obj
    ON f.objective_key = d_obj.objective_key

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