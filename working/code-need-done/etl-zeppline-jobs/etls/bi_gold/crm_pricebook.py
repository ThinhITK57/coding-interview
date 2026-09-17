%livy.pyspark

sql_query = """
SELECT 
    product_id as product_id,
    product_name,
    product_category,
    product_version,
    item_type,
    is_active,
    product_description,
    sku,
    product_type,
    sub_type,
    product_license,
    price_type,
    package,
    price,
    currency_code,
    price_min,
    price_max,
    price_unit,
    is_quantity_based,
    is_related_csmp,
    csmp_discount,
    csmp_discount_silver,
    csmp_discount_gold,
    csmp_discount_diamond,
    catalog_id,
    related_pricebook,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM bi_silver.crm_pricebook
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_pricebook")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_pricebook"

# hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)

# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_pricebook")
