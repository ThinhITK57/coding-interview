%livy.pyspark
spark.catalog.clearCache()

sql_query = """
SELECT distinct territory_name FROM crm_silver.dim_territory_name
"""

df = spark.sql(sql_query)

location = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/dim_territory_name"

(
    df.repartition(1).write
    .mode("overwrite")
    .format("parquet")
    .option("path", location)
    .saveAsTable("bi_silver.dim_territory_name")
)