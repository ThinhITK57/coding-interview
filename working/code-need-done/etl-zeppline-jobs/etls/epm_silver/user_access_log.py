%livy.pyspark

from pyspark.sql.types import StructType, StructField, StringType, DateType

tgt_table = "bi_silver.epm_user_access_log"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_user_access_log"

# 1) Kiểm tra nếu bảng raw tồn tại thì nạp, nếu chưa thì tạo bảng rỗng đúng schema
if spark.catalog.tableExists("epm_raw_snapshot", "user_access_log"):
    spark.sql("REFRESH TABLE epm_raw_snapshot.user_access_log")
    df = spark.sql("""
        SELECT
            TO_DATE(COALESCE(login_date, login_timestamp)) AS login_date,
            CAST(name AS STRING) AS name,
            CAST(first_name AS STRING) AS first_name,
            CAST(last_name AS STRING) AS last_name,
            CAST(groups AS STRING) AS groups,
            CAST(direct_manager AS STRING) AS direct_manager,
            CAST(job_title AS STRING) AS job_title
        FROM epm_raw_snapshot.user_access_log
    """)
else:
    schema = StructType([
        StructField("login_date", DateType(), True),
        StructField("name", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("groups", StringType(), True),
        StructField("direct_manager", StringType(), True),
        StructField("job_title", StringType(), True)
    ])
    df = spark.createDataFrame([], schema)

# 2) Lưu Parquet và đăng ký bảng Hive
df.write.mode("overwrite").format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

# 3) Tạo View tương thích ngược cho epm_silver.user_access_log
spark.sql(f"CREATE OR REPLACE VIEW epm_silver.user_access_log AS SELECT * FROM {tgt_table}")

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table}")
