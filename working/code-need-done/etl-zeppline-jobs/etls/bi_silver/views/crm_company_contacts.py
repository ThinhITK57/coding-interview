%livy.pyspark

# spark.sql("DROP VIEW IF EXISTS bi_silver.crm_company_contacts")


spark.sql("""
CREATE OR REPLACE VIEW bi_silver.crm_company_contacts  AS
SELECT 
    company_id,
    upper(company_name) as company_name,
    created_at,
    updated_at
    
FROM bi_silver.company_contacts
""")
