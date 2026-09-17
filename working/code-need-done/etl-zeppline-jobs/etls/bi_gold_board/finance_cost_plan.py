%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.finance_cost_plan")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_cost_plan  AS
SELECT 
    CAST(plan_date as TIMESTAMP ) as plan_date,
    UPPER(cost_group) as cost_group,
    expected_cost_value
FROM bi_silver.finance_cost_plan
""")
