%livy.pyspark

from pyspark.sql.types import StructType, StructField, StringType, DateType

tgt_table = "bi_gold.user_access_traffic"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_gold/data/user_access_traffic"

# Kiểm tra nếu bảng user_access_log tồn tại, nếu chưa có tạo bảng rỗng đúng schema contract
if spark.catalog.tableExists("bi_silver", "epm_user_access_log"):
    spark.sql("REFRESH TABLE bi_silver.epm_user_access_log")
    sql_query = """
    SELECT
        TO_DATE(login_date) AS login_date,
        CAST(name AS STRING) AS name,
        CAST(first_name AS STRING) AS first_name,
        CAST(last_name AS STRING) AS last_name,
        CAST(groups AS STRING) AS groups,
        CAST(direct_manager AS STRING) AS direct_manager,
        CAST(job_title AS STRING) AS job_title
    FROM bi_silver.epm_user_access_log
    """
    df = spark.sql(sql_query)
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

df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

# Tạo alias tương thích ngược
spark.sql(f"CREATE OR REPLACE VIEW bi_silver.epm_mart_user_access_traffic AS SELECT * FROM {tgt_table}")

spark.catalog.refreshTable(tgt_table)
print(f"DONE: {tgt_table}")