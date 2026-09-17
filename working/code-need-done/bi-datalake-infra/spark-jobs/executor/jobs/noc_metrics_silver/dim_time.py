# %livy.pyspark

spark.sql("drop  table IF EXISTS noc_metrics_silver.dim_time")
 
sql_query = """

SELECT DISTINCT
     metric_ts,
    CAST(from_unixtime(metric_ts / 1000) AS TIMESTAMP)         AS metric_datetime,
    hour(CAST(from_unixtime(metric_ts / 1000) AS TIMESTAMP))  AS hour,
    minute(CAST(from_unixtime(metric_ts / 1000) AS TIMESTAMP)) AS minute
FROM noc_metrics_silver.metrics


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/noc-metrics-silver/data/dim_time"
    ) \
    .saveAsTable("noc_metrics_silver.dim_time")
