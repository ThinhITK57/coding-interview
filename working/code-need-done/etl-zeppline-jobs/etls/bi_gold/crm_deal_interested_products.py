%livy.pyspark

sql_query = """
SELECT
  md5(concat_ws('|',
    coalesce(CAST(deal_id AS STRING),'__NULL__'),
    coalesce(CAST(interested_product_category AS STRING),'__NULL__')
  )) AS id,
  deal_id,
  interested_product_category
FROM bi_silver.crm_deal_interested_products
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_deal_interested_products")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_deal_interested_products"
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)
# if fs.exists(path):
#     fs.delete(path, True)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", tgt_path)
  .saveAsTable("bi_gold.crm_deal_interested_products")
)

print("DONE")
