%livy.pyspark

sql_query = """
SELECT DISTINCT
    row_number() OVER (
        ORDER BY 
            quotation_id,
            CAST(deal_id AS STRING),
            product_name,
            unit,
            duration,
            package
    ) AS id,

    quotation_id,
    CAST(deal_id AS STRING) AS deal_id,

    product_name,
    base_price as price,
    quantitative,
    unit,
    duration,
    package,
    vat,
    discount,
    discount_type,
    final_total_amount,
    base_total_amount
FROM bi_silver.crm_deal_quotation_products
WHERE quotation_id IS NOT NULL
  AND deal_id IS NOT NULL
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_deal_quotation_products")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_deal_quotation_products"

# Hard delete parquet folder (avoid doubles)
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)
# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_deal_quotation_products")
