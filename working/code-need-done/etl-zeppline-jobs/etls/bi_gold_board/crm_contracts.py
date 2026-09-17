%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_contracts")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_contracts  AS
SELECT 
    CAST(sign_date AS TIMESTAMP) as sign_date,
    CAST(fac_date  AS TIMESTAMP) as fac_date,
    CAST(expire_date  AS TIMESTAMP) as expire_date,
    upper(contract_type) as contract_type,
    upper(signing_method) as signing_method,
    upper(customer_segment_l1) as customer_segment_l1,
    upper(customer_segment_l2) as customer_segment_l2,
    upper(customer_segment_l3) as customer_segment_l3,
    upper(status) as contract_status,
    upper(customer_group) as customer_group,
    upper(currency_code) as currency_code,
    contract_amount,
    vcs_contract_amount,
    viettel_contract_amount,
    
    deal_id,
    id
    
FROM bi_silver.crm_contracts
""")
