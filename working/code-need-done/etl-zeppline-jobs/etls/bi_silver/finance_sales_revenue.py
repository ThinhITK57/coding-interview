%livy.pyspark

target_table = "bi_silver.finance_sales_revenue"
target_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_sales_revenue"

# 1) Config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.catalog.clearCache()


# Đọc từ bảng nguồn
sql_plan = """
   SELECT
    CAST(invoice_date as TIMESTAMP) as invoice_date,
    CASE WHEN invoice_type is null OR invoice_type = '' THEN 'Khác' ELSE invoice_type  END as invoice_type,
    billing_description,
    CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(revenue_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS revenue_amount,
    CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(revenue_share_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS revenue_share_amount,
    CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(final_revenue_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS final_revenue_amount,
    CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(vat_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS vat_amount,
    CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(gross_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS gross_amount,
    
    CASE WHEN customer_type is null OR customer_type = '' THEN 'Khác' ELSE customer_type  END as revenue_source_name,
    CASE WHEN customer_channel is null OR customer_channel = '' THEN 'Khác' ELSE customer_channel  END as customer_channel,
    CASE WHEN customer_group is null OR customer_group = '' THEN 'Khác' ELSE customer_group  END as customer_segment_l3,
    CASE WHEN department is null OR department = '' THEN 'Khác' ELSE department  END as department_alias,
    CASE WHEN customer_name is null OR customer_name = '' THEN 'Khác' ELSE customer_name  END as company_alias,
    CASE WHEN customer_group is null OR customer_group = '' THEN 'Khác' ELSE customer_group  END as customer_industry,
    CASE WHEN account_manager is null OR account_manager = '' THEN 'Khác' ELSE account_manager  END as account_manager,
    CASE WHEN presale is null OR presale = '' THEN 'Khác' ELSE presale  END as presale,
    CASE WHEN service_category is null OR service_category = '' THEN 'Khác' ELSE service_category  END as service_category,
    CASE WHEN revenue_recognition_type is null OR revenue_recognition_type = '' THEN 'Khác' ELSE revenue_recognition_type  END as revenue_recognition_type,
    CASE WHEN customer_scope is null OR customer_scope = '' THEN 'Khác' ELSE customer_scope  END as customer_segment_l1,
    CASE WHEN service_name is null OR service_name = '' THEN 'Khác' ELSE service_name  END as service_name,
    CASE WHEN revenue_type is null OR revenue_type = '' THEN 'Khác' ELSE revenue_type  END as revenue_type,
    CASE WHEN mss_shared_revenue is null OR mss_shared_revenue = '' THEN 'No' ELSE mss_shared_revenue  END as mss_shared_revenue_mode,
    CASE WHEN soc_type is null OR soc_type = '' THEN 'No' ELSE soc_type  END as soc_type,
    CASE WHEN region is null OR region = '' THEN 'Khác' ELSE region  END as region,
    CASE WHEN service_code is null OR service_code = '' THEN '' ELSE service_code  END as product_category_code,
    'VND' as currency_code

FROM finance_raw.sales_revenue
"""

# Thực thi và lưu trữ
df_final = spark.sql(sql_plan)

df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)
    # .option("path", target_path) \
    # .saveAsTable(target_table)
    
# .save(target_path)
# .option("path", target_path) \
#     .saveAsTable(target_table)

# Refresh metadata
# spark.sql("REFRESH TABLE " + target_table)

print("--- FINISHED: finance_sales_product_revenue ---")