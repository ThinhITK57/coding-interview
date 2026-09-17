# %livy.pyspark

spark.sql("drop  table IF EXISTS noc_metrics_silver.dim_entity")
 
sql_query = """

SELECT DISTINCT
    customer,
    project,
    host
FROM noc_metrics_silver.metrics
WHERE customer IS NOT NULL
   OR project IS NOT NULL
   OR host IS NOT NULL

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/noc-metrics-silver/data/dim_entity"
    ) \
    .saveAsTable("noc_metrics_silver.dim_entity")
