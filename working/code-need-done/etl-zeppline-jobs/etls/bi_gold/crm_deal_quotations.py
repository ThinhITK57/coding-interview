%livy.pyspark

sql_query = """
SELECT DISTINCT
    row_number() OVER (
        ORDER BY deal_id, quotation_id, created_at
    ) AS id,
    quotation_id,
    deal_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    status,
    currency_code,
    global_discount,
    global_discount_type
FROM bi_silver.crm_deal_quotations
WHERE deal_id IS NOT NULL
  AND quotation_id IS NOT NULL
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_deal_quotations")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_deal_quotations"

# # Hard delete parquet folder (avoid doubles)
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)
# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_deal_quotations")
