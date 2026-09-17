%livy.pyspark

spark.sql("DROP VIEW IF EXISTS bi_gold.crm_payment_forecast")


spark.sql("""
CREATE OR REPLACE VIEW bi_gold.crm_payment_forecast (
    payment_date COMMENT 'Invoice issue date',
    due_date COMMENT 'Expected cash collection date (forecast date)',
    product_type COMMENT 'Product type',
    deployment_type COMMENT 'Deployment model of the product/service',
    product_category COMMENT 'Product category',
    territory_name COMMENT 'Sales territory name',
    market_name COMMENT 'Market name (same as territory_name)',
    customer_group COMMENT 'Customer group classification',
    customer_segment_l1 COMMENT 'Customer segmentation level 1',
    customer_segment_l2 COMMENT 'Customer segmentation level 2',
    customer_segment_l3 COMMENT 'Customer segmentation level 3',
    currency_code COMMENT 'Transaction currency code',
    is_recurring COMMENT 'Indicates recurring revenue',
    is_actual COMMENT 'Flag showing whether revenue is actual or forecast',
    vnd_amount COMMENT 'Revenue amount in VND',
    currency_amount COMMENT 'Revenue amount in original currency',
    contract_id COMMENT 'contract_id',
    deal_id COMMENT 'deal_id'
)
COMMENT 'Forecast revenue dataset based on forecast invoices. Used for revenue forecast and cash collection planning.'
AS
SELECT 
    CAST(payment_date as TIMESTAMP) as payment_date,
    CAST(due_date as TIMESTAMP) as due_date,
    UPPER(product_type) as product_type,
    UPPER(deployment_type) as deployment_type,
    UPPER(product_category) as product_category,
    UPPER(territory_name) as territory_name,
    UPPER(territory_name) as market_name,
    UPPER(COALESCE(customer_group, 'Unknown')) as customer_group,
    UPPER(customer_segment_l1) as customer_segment_l1,
    UPPER(customer_segment_l2) as customer_segment_l2,
    UPPER(COALESCE(customer_segment_l3, 'Unknown')) as customer_segment_l3,
    UPPER(currency_code) as currency_code,
    is_recurring,
    is_actual,
    vnd_amount,
    currency_amount,
    contract_id,
    deal_id
FROM bi_silver.crm_payment_forecast
""")