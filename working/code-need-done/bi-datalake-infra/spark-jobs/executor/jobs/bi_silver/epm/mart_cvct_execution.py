# %livy.pyspark

tgt_table = "bi_gold.cvct_execution_report"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_gold/data/cvct_execution_report"

spark.sql("REFRESH TABLE bi_silver.fact_epm_cvct_execution_snapshot")
spark.sql("REFRESH TABLE bi_silver.dim_epm_resource")

sql_query = """
WITH latest_fact AS (
    SELECT *
    FROM bi_silver.fact_epm_cvct_execution_snapshot
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM bi_silver.fact_epm_cvct_execution_snapshot
    )
)
SELECT
    f.assignee,
    COALESCE(d_res.resource_name, 'Unknown') AS assignee_name,

    f.resources,
    f.name,
    f.status,
    f.due_date,
    f.percent_completed,
    f.target_value_m,
    f.target_date_m

FROM latest_fact f

-- JOIN với dim_epm_resource để chuẩn hóa tên nhân sự
LEFT JOIN bi_silver.dim_epm_resource d_res
    ON f.assignee_key = d_res.resource_key
"""

df = spark.sql(sql_query)

df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

# Tạo alias tương thích ngược
spark.sql(f"CREATE OR REPLACE VIEW bi_silver.epm_mart_cvct_execution_report AS SELECT * FROM {tgt_table}")
spark.sql(f"CREATE OR REPLACE VIEW bi_silver.epm_mart_dieu_hanh_cvct_klcd AS SELECT * FROM {tgt_table}")

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table}")
