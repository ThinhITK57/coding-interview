# %livy.pyspark

spark.sql("drop  table IF EXISTS noc_metrics_silver.dim_metric_grain")
 
sql_query = """
SELECT DISTINCT
    metric_name,
    agg_type,
    CASE
        WHEN metric_ts % 3600 = 0 THEN 'hourly'
        WHEN metric_ts % 300  = 0 THEN '5min'
        ELSE 'raw'
    END AS grain
FROM noc_metrics_silver.metrics

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/noc-metrics-silver/data/dim_metric_grain"
    ) \
    .saveAsTable("noc_metrics_silver.dim_metric_grain")
