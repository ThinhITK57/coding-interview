%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.dim_unit_level_1")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.dim_unit_level_1  AS
SELECT 
    *
FROM bi_silver.dim_unit_level_1
""")
