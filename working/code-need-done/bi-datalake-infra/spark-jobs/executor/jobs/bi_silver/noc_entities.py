# %livy.pyspark

# =========================
# Config
# =========================
target_table = "bi_silver.noc_entities"
target_path  = "s3a://bi-silver/noc_entities"

# =========================
# 1) Hard delete HDFS path (avoid leftover files)
# =========================
# jvm = spark._jvm
# hconf = spark._jsc.hadoopConfiguration()
# fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = jvm.org.apache.hadoop.fs.Path(target_path)

# if fs.exists(path):
#     fs.delete(path, True)   # recursive=True

# =========================
# 2) Drop table (remove Hive/Metastore metadata)
# =========================
#spark.sql(f"DROP TABLE IF EXISTS {target_table}")

# =========================
# 3) Build dataframe
# =========================
sql_query = """

SELECT DISTINCT
    customer,
    project,
    host
FROM bi_silver.noc_metrics
WHERE customer IS NOT NULL
   OR project IS NOT NULL
   OR host IS NOT NULL

"""
df = spark.sql(sql_query)

print("Rows:", df.count())
df.show(10, truncate=False)

# =========================
# 4) Write parquet + create external table at path
# =========================
(df.repartition(1).write \
  .format("parquet")
  .mode("overwrite")
  .option("path", target_path)
  .saveAsTable(target_table)
)


