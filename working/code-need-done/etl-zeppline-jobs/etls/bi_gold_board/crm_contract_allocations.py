%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_contract_allocations")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_contract_allocations  AS
SELECT 
    CAST(first_payment_date AS TIMESTAMP) as first_payment_date,
    CAST(invoice_activation_date  AS TIMESTAMP) as invoice_activation_date,
    CAST(fac_date  AS TIMESTAMP) as fac_date,
    CAST(due_date  AS TIMESTAMP) as due_date,
    upper(product_category) as product_category,
    upper(product_type) as product_type,
    upper(deployment_type) as deployment_type,
    upper(customer_group) as customer_group,
    upper(territory_name) as territory_name,
    upper(territory_name) as market_name,
    upper(customer_segment_l1) as customer_segment_l1 ,
    upper(customer_segment_l2) as customer_segment_l2 ,
    upper(customer_segment_l3) as customer_segment_l3,
    upper(currency_code) as currency_code,
    is_recurring,
    period_number,
    upper(period_name) as period_name,
    period_value,
    product_total_value,
    sales_performance_value,
    
    contract_id
    
FROM bi_silver.crm_contract_allocations
""")
