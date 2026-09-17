%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_deal_reason_stats")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_deal_reason_stats  AS
SELECT 
    UPPER(reason_name) as reason_name ,
    count(*) as number_of_deals
    
FROM bi_silver.crm_deal_reasons
Group by 1
""")
