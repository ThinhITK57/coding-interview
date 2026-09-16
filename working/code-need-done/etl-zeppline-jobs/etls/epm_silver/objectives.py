%livy.pyspark

tgt_table = "bi_silver.epm_objectives"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_objectives"

# 1) Hỗ trợ cả 2 tên bảng raw có 's' hoặc không có 's'
raw_tbl = "epm_raw_snapshot.objectives"
if not spark.catalog.tableExists("epm_raw_snapshot", "objectives"):
    raw_tbl = "epm_raw_snapshot.objective"

spark.sql("REFRESH TABLE " + raw_tbl)

# 2) Deduplicate bằng CTE Window Function
sql_query = f"""
WITH ranked_objectives AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sysid
            ORDER BY COALESCE(last_updated_on, created_on) DESC, sysid DESC
        ) AS rn
    FROM {raw_tbl}
)
SELECT *
FROM ranked_objectives
WHERE rn = 1
"""

df = spark.sql(sql_query).drop("rn")

# 3) Lưu Parquet và đăng ký bảng Hive
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tgt_table)

# 4) Tạo View tương thích ngược cho epm_silver.objectives
spark.sql(f"CREATE OR REPLACE VIEW epm_silver.objectives AS SELECT * FROM {tgt_table}")

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table} and view epm_silver.objectives")