%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_deal_quotation_products")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_deal_quotation_products  AS
SELECT 
    CAST(quotation_created_at AS TIMESTAMP) as quotation_date ,
    upper(quotation_status) as quotation_status,
    upper(product_name) as product_name,
    global_discount_type,
    global_discount as  global_discount_amount,
    upper(package) as package,
    base_price,
    quantitative,
    unit as product_unit,
    upper(discount_type) as discount_type,
    discount as discount_amount,
    final_total_amount,
    base_total_amount,
    
    deal_id
    
FROM bi_silver.crm_deal_quotation_products
""")
