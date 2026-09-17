# %livy.pyspark

spark.sql("drop  table IF EXISTS noc_metrics_silver.dim_metric")
 
sql_query = """

SELECT DISTINCT
    metric_name,
    agg_type
FROM noc_metrics_silver.metrics
WHERE metric_name IS NOT NULL


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/noc-metrics-silver/data/dim_metric"
    ) \
    .saveAsTable("noc_metrics_silver.dim_metric")
