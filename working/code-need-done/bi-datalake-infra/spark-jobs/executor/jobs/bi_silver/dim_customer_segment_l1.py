# %livy.pyspark
spark.catalog.clearCache()
sql_query = """
SELECT distinct segment_l1 FROM bi_silver.crm_deals
"""

df = spark.sql(sql_query)

location = "s3a://bi-silver/dim_customer_segment_l1"

(
    df.repartition(1).write
    .mode("overwrite")
    .format("parquet")
    .option("path", location)
    .saveAsTable("bi_silver.dim_customer_segment_l1")
)