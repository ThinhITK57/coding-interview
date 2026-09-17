%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.finance_product_revenue_plan")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_product_revenue_plan  AS
SELECT 
    CAST(plan_date as TIMESTAMP ) as plan_date,
    UPPER(product_group) as product_group,
    UPPER(product_name) as product_name,
    plan_must_amount,
    plan_nice_amount
FROM bi_silver.finance_product_revenue_plan
""")
