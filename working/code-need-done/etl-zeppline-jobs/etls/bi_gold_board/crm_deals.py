%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_deals")

# created_at
# updated_at
# due_date
# sign_date
# last_assigned_at
# fac_date
# expire_date
# stage_updated_time
# closed_date
# expected_close_date

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_deals  AS
SELECT 
    CAST(created_at as TIMESTAMP) as deal_date,
    CAST(due_date as TIMESTAMP) as due_date,
    CAST(sign_date as TIMESTAMP) as sign_date,
    CAST(last_assigned_at as TIMESTAMP) as last_assigned_date,
    CAST(fac_date as TIMESTAMP) as fac_date,
    CAST(stage_updated_time as TIMESTAMP) as stage_updated_date,
    CAST(closed_date as TIMESTAMP) as closed_date,
    CAST(expected_close_date as TIMESTAMP) as expected_close_date,
    UPPER(customer_group) as customer_group,
    UPPER(customer_segment_l1) as customer_segment_l1,
    UPPER(customer_segment_l2) as customer_segment_l2,
    UPPER(customer_segment_l3) as customer_segment_l3,
    UPPER(channel) as deal_channel,
    UPPER(territory_name) as territory_name,
    UPPER(territory_name) as market_name,
    UPPER(deal_status) as deal_status,
    UPPER(deal_stage_name) as deal_stage_name,
    UPPER(probability_bucket) as probability_bucket,
    UPPER(company_province) as company_province,
    UPPER(currency_code) as currency_code,
    expected_deal_value,
    budget_amount,
    currency_amount,
    vnd_amount,
    probability,
    
    deal_id,
    contract_id,
    UPPER(company_name) as customer_name,
    UPPER(contract_type) as contract_type
    
FROM bi_silver.crm_deals
""")
