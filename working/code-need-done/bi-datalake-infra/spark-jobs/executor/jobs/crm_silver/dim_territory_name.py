# %livy.pyspark

sql_query = """
SELECT distinct name as territory_name FROM crm_raw.territories
"""

df = spark.sql(sql_query)

location = "s3a://vcs-silver/crm-silver/dim_territory_name"

(
    df.repartition(1).write
    .mode("overwrite")
    .format("parquet")
    .option("path", location)
    .saveAsTable("crm_silver.dim_territory_name")
)