%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.dim_customer_segment_l1")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.dim_customer_segment_l1  AS
SELECT 
    *
FROM bi_silver.dim_customer_segment_l1
""")
