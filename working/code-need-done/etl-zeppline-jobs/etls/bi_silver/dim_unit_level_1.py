%livy.pyspark
spark.catalog.clearCache()

sql_query = """
SELECT distinct unit_level_1 FROM hive.bi_silver.hr_employee_onboard
"""

df = spark.sql(sql_query)

location = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/dim_unit_level_1"

(
    df.repartition(1).write
    .mode("overwrite")
    .format("parquet")
    .option("path", location)
    .saveAsTable("bi_silver.dim_unit_level_1")
)