%livy.pyspark

target_table = "bi_gold.cx_sur_surveys"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/cx_sur_surveys"

# hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))


sql_query = """
SELECT 
  CAST(survey_id AS STRING) AS survey_id,
  survey_type,
  survey_name,
  folder,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(enabled AS BOOLEAN) AS enabled,
  journey,
  customer_touchpoint,
  product_category
FROM bi_silver.cx_sur_surveys
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")