%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.finance_revenue_plan")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_revenue_plan  AS
SELECT 
    CAST(plan_date as TIMESTAMP ) as plan_date,
    UPPER(customer_segment_l1) as customer_segment_l1,
    UPPER(customer_segment_l2) as customer_segment_l2,
    UPPER(segment_l1_alias) as customer_segment_alias,
    plan_viettel_group_amount,
    plan_must_amount,
    plan_nice_amount
FROM bi_silver.finance_revenue_plan
""")
