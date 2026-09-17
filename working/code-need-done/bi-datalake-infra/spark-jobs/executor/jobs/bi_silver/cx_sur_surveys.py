# %livy.pyspark
from pyspark.sql import functions as F

target_path = "s3a://bi-silver/cx_sur_surveys"
target_table = "bi_silver.cx_sur_surveys"

# 1) Drop table metadata
# spark.sql("DROP TABLE IF EXISTS {0}".format(target_table))

# 2) Hard delete folder (HDFS-safe, Livy-safe)
# print("Deleting old path:", target_path)

# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)

# if fs.exists(path):
#     fs.delete(path, True)
spark.catalog.clearCache()
# print("Path cleaned!")

# 3) Query
sql_query = """
SELECT DISTINCT 
    sd.survey_id,
    CASE WHEN sd.type = '' THEN 'N/A' ELSE  COALESCE(sd.type, 'N/A') END as survey_type,
    CASE WHEN sd.name = '' THEN 'N/A' ELSE  COALESCE(sd.name, 'N/A') END as survey_name,
    CASE WHEN sd.folder = '' THEN 'N/A' ELSE  COALESCE(sd.folder, 'N/A') END as folder_name,
    CAST(sd.created_at as TIMESTAMP) AS created_at,
    sd.enabled,
    CASE WHEN scj.journey = '' THEN 'N/A' ELSE  COALESCE(scj.journey, 'N/A') END as journey,
    CASE WHEN scj.customer_touchpoint = '' THEN 'N/A' ELSE  COALESCE(scj.customer_touchpoint, 'N/A') END as customer_touchpoint,
    CASE WHEN scj.product_category = '' THEN 'N/A' ELSE  COALESCE(scj.product_category, 'N/A') END as product_category
    
FROM cx_survicate_raw.dim_survicate_survey_details sd
LEFT JOIN cx_cso_raw.dim_surveys_customer_journey scj
    ON sd.survey_id = scj.survey_id
"""

df = spark.sql(sql_query)

# 4) Write (Spark 2.0 stable)
df.write \
  .mode("overwrite") \
  .format("parquet") \
  .save(target_path)
#   .option("path", target_path) \
#   .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("DONE. Rows:", df.count())
