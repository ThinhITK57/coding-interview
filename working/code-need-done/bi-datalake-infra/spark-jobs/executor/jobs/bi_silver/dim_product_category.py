# %livy.pyspark
spark.catalog.clearCache()

sql_query = """
select distinct  product_category from (
select distinct product_category  FROM bi_silver.crm_contract_allocations
union all
SELECT distinct product_category FROM bi_silver.finance_cash_collection
)

"""

df = spark.sql(sql_query)

location = "s3a://bi-silver/dim_product_category"

(
    df.repartition(1).write
    .mode("overwrite")
    .format("parquet")
    .option("path", location)
    .saveAsTable("bi_silver.dim_product_category")
)