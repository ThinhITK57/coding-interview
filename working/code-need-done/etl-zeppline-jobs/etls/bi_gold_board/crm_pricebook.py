%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_pricebook")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_pricebook  AS
SELECT 
    CAST(created_at as TIMESTAMP) as created_at ,
    CAST(updated_at as TIMESTAMP) as updated_at ,
    UPPER(product_category) as product_category,
    UPPER(product_version) as product_version,
    UPPER(item_type) as item_type,
    UPPER(product_type) as product_type,
    UPPER(sub_type) as sub_type,
    UPPER(product_license) as product_license,
    UPPER(price_type) as price_type,
    UPPER(package) as package,
    UPPER(currency_code) as currency_code,
    is_quantity_based,
    is_related_csmp,
    csmp_discount,
    csmp_discount_silver,
    csmp_discount_gold,
    csmp_discount_diamond,
    price_unit,
    price,
    price_min,
    price_max
    
FROM bi_silver.crm_pricebook
""")
