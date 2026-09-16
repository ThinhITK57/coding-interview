from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()
)

schema = "#SCHEMA#"
table = "#TABLE_NAME#"
snapshot_ts = "#SNAPSHOT_TS#"
silver_batch_id = "#SILVER_BATCH_ID#"

spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")

sql_query = """
#SQL_QUERY#
"""

df = spark.sql(sql_query)
df = (
    df
    .withColumn("silver_snapshot_at_ts",lit(snapshot_ts).cast("timestamp"))
    .withColumn("silver_batch_id", lit(silver_batch_id))
)

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .option(
        "path",
        f"/opt/datasets/crawlers/vcs/{schema}/data/{table}"
    )
    .saveAsTable(f"{schema}.{table}")
)

spark.stop()
