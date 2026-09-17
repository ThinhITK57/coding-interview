%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.finance_production_cost_allocations")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_production_cost_allocations  AS
SELECT 
    CAST(report_date as TIMESTAMP ) as report_date,
    UPPER(old_category) as old_product_category,
    UPPER(cost_group) as cost_group,
    UPPER(territory_name) as territory_name,
    UPPER(market_name) as market_name,
    base_currency_amount
FROM bi_silver.finance_production_cost_allocations
""")

