%livy.pyspark

sql_query = """
SELECT DISTINCT
    id,
    reason_name
FROM bi_silver.crm_deal_reasons
WHERE id IS NOT NULL
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_deal_reasons")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_deal_reasons"

# Hard delete parquet folder
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)

# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_deal_reasons")
