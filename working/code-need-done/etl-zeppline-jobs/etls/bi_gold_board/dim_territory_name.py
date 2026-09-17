%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.dim_territory_name")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.dim_territory_name  AS
SELECT 
    *
FROM bi_silver.dim_territory_name
""")
