%livy.pyspark

# spark.sql("DROP VIEW IF EXISTS bi_gold.finance_actual_cost")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_actual_cost  AS
SELECT 
    CAST(report_date as TIMESTAMP) as report_date,
    UPPER(old_category) as old_category,
    UPPER(product_category_code) as product_category_code,
    UPPER(cost_group) as cost_group,
    UPPER(territory_name) as territory_name,
    UPPER(product_category) as product_category,
    UPPER(business_unit_level_1) as business_unit_level_1,
    base_currency_amount
FROM bi_silver.finance_actual_cost
""")
