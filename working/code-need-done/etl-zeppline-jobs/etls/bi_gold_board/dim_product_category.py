%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.dim_product_category")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.dim_product_category  AS
SELECT 
    *
FROM bi_silver.dim_product_category
""")
