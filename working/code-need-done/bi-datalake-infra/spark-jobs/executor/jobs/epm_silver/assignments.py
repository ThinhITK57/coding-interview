# %livy.pyspark

tgt_table = "bi_silver.epm_assignments"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_assignments"

# 1) Refresh metadata nguồn
spark.sql("REFRESH TABLE epm_raw_snapshot.assignments")

# 2) Deduplicate bằng CTE Window Function
sql_query = """
WITH ranked_assignments AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sysid
            ORDER BY COALESCE(last_updated_on, created_on) DESC, sysid DESC
        ) AS rn
    FROM epm_raw_snapshot.assignments
)
SELECT *
FROM ranked_assignments
WHERE rn = 1
"""

df = spark.sql(sql_query).drop("rn")

# 3) Lưu Parquet và đăng ký bảng Hive
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tgt_table)

# 4) Tạo View tương thích ngược cho epm_silver.assignments
spark.sql(f"CREATE OR REPLACE VIEW epm_silver.assignments AS SELECT * FROM {tgt_table}")

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table} and view epm_silver.assignments")