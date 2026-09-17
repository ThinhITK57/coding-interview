# %livy.pyspark

spark.sql("drop  table IF EXISTS noc_metrics_silver.dim_date")
 
sql_query = """

SELECT DISTINCT
    dt,
    year(dt)  AS year,
    month(dt) AS month,
    day(dt)   AS day
FROM noc_metrics_silver.metrics
WHERE dt IS NOT NULL


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/noc-metrics-silver/data/dim_date"
    ) \
    .saveAsTable("noc_metrics_silver.dim_date")
