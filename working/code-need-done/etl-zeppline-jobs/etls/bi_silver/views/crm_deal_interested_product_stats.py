%livy.pyspark

# spark.sql("DROP VIEW IF EXISTS bi_silver.crm_deal_interested_product_stats")


spark.sql("""
CREATE OR REPLACE VIEW bi_silver.crm_deal_interested_product_stats  AS
SELECT 
    UPPER(interested_product_category) as product_category,
    count(*) as number_of_deals
    
FROM bi_silver.crm_deal_interested_products
group by 1
""")
