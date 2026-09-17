%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.dim_date")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.dim_date  AS
SELECT 
    *
FROM bi_silver.dim_date
""")
