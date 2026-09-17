%livy.pyspark

# Thành phần: cm_catalog, cm_pricebook
spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


tgt_table = "bi_silver.crm_product_tree_item_code"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_product_tree_item_code"

# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_product_tree_item_code_item_code")

query = """
SELECT 
    CONCAT(
        COALESCE(CAST(product_group AS STRING), ''), '__',
        COALESCE(CAST(product_sub_group AS STRING), ''), '__',
        COALESCE(CAST(category_code AS STRING), ''), '__',
        COALESCE(CAST(product_category AS STRING), ''), '__',
        COALESCE(CAST(sku AS STRING), ''), '__',
        COALESCE(CAST(version AS STRING), ''), '__',
        COALESCE(CAST(item_type AS STRING), ''), '__',
        COALESCE(CAST(license AS STRING), ''), '__',
        COALESCE(CAST(price_type AS STRING), ''), '__',
        COALESCE(CAST(unit AS STRING), '')
    ) AS idd,
    
    CONCAT(
        CAST(category_code AS STRING), '_',
        LPAD(CAST(ROW_NUMBER() OVER (
            PARTITION BY category_code 
            ORDER BY product_category, sku, version
        ) AS STRING), 3, '0')
    ) AS item_code
FROM bi_silver.crm_product_tree
"""

df = spark.sql(query)

# 1) Drop table (KHÔNG dùng f-string để tránh lỗi Python2)
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

