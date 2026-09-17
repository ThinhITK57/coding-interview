%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_partners")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_partners  AS
SELECT 
    id as partner_id,
    UPPER(COALESCE(country, 'Unknown')) as country,
    UPPER(COALESCE(partner_type, 'Unknown')) as partner_type,
    UPPER(COALESCE(partner_status, 'Unknown')) as partner_status,
    UPPER(COALESCE(document_type, 'Unknown')) as document_type,
    effective_date,
    expiration_date
FROM bi_silver.crm_partners
""")
