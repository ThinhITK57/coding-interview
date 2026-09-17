%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_allocated_revenue")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_allocated_revenue AS
SELECT 
    CAST(report_date as TIMESTAMP ) as report_date,
    UPPER(product_category_code) as product_category_code,
    UPPER(product_category) as product_category,
    UPPER(product_service_group) as product_service_group,
    UPPER(revenue_type) as revenue_type,
    UPPER(channel_name) as channel_name,
    UPPER(department_alias) as sales_department,
    UPPER(customer_segment_l1) as customer_segment_l1,
    UPPER(customer_segment_l3) as customer_segment_l3,
    UPPER(revenue_group) as revenue_group,
    UPPER(payment_stage) as payment_stage,
    UPPER(business_unit_level_1) as business_unit_level_1,
    UPPER(territory_name) as territory_name,
    
    revenue_amount
FROM bi_silver.crm_allocated_revenue
""")