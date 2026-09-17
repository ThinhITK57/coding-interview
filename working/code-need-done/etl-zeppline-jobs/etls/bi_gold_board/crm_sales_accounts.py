%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_sales_accounts")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_sales_accounts  AS
SELECT 
    id,
    UPPER(country) as country,
    UPPER(state) as state,
    UPPER(city) as city,
    UPPER(customer_segment_l1) as customer_segment_l1,
    UPPER(customer_segment_l2) as customer_segment_l2,
    UPPER(COALESCE(customer_segment_l3, 'Unknown')) as customer_segment_l3,
    CAST(created_at as TIMESTAMP) as created_at,
    UPPER(business_type) as business_type,
    UPPER(industry_type) as industry_type,
    using_soc
    
FROM bi_silver.crm_sales_accounts
""")
