%livy.pyspark
spark.sql("""
CREATE OR REPLACE VIEW bi_gold.cx_sur_surveys AS
select
    created_at,
    
    UPPER(survey_type) as survey_type,
    UPPER(folder_name) as folder_name,
    UPPER(journey) as journey,
    UPPER(customer_touchpoint) as customer_touchpoint,
    UPPER(product_category) as product_category,
    
    
    survey_id
    
FROM bi_silver.cx_sur_surveys
""")
