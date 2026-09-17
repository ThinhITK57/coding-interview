# %livy.pyspark


# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_sales_product_revenue")

# # 1) Config
# spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
# spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# spark.catalog.clearCache()

# tgt_cost_table = "bi_silver.finance_sales_product_revenue"
# tgt_cost_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_sales_product_revenue"

# # Đọc từ bảng nguồn
# sql_plan = """
#    SELECT
#     -- giả lập ID
#     md5(concat_ws('|',
#        'ALL',
#         billing_description,
#         customer_name,
#         service_code
#     )) AS contract_id,

#     md5(concat_ws('|',
#     'ALL',
#         billing_description,
#         customer_name,
#         service_code,
#         invoice_date
#     )) AS invoice_id,

#     -- tag nguồn
#     'ALL' AS invoice_tag,

#     -- giữ nguyên các field
#     CAST(invoice_date as TIMESTAMP) as invoice_date,
#     CAST(invoice_month as BIGINT) as invoice_month,
#     invoice_type,
#     billing_description,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(revenue_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS revenue_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(revenue_share_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS revenue_share_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(final_revenue_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS final_revenue_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(vat_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS vat_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(gross_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS gross_amount,
#     revenue_recognition_type,
#     service_category,
#     customer_scope as customer_segment_l1,
#     customer_type as revenue_source_name,
#     customer_channel,
#     customer_name as company_alias,
#     customer_segment as customer_segment_l3,
#     department as department_alias,
#     customer_group as customer_industry,
#     account_manager,
#     presale,
#     service_name,
#     revenue_type,
#     soc_type,
#     mss_shared_revenue,
#     market_scope,
#     service_code,
#     region,
#     note,
#     current_account_manager,
#     is_vvip_customer,
#     is_master_contract,
#     'VND' as currency_code

# FROM finance_raw.sales_revenue_share_all

# UNION ALL

# SELECT
#     md5(concat_ws('|',
#     'SOC',
#         billing_description,
#         customer_name,
#         service_code
#     )) AS contract_id,

#     md5(concat_ws('|',
#      'SOC',
#         billing_description,
#         customer_name,
#         service_code,
#         invoice_date
#     )) AS invoice_id,

#     'SOC' AS invoice_tag,

#     CAST(invoice_date as TIMESTAMP) as invoice_date,
#     CAST(invoice_month as BIGINT) as invoice_month,
#     invoice_type,
#     billing_description,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(revenue_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS revenue_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(revenue_share_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS revenue_share_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(final_revenue_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS final_revenue_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(vat_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS vat_amount,
#     CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(gross_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS gross_amount,
#     revenue_recognition_type,
#     service_category,
#     customer_scope  as customer_segment_l1,
#     customer_type as revenue_source_name,
#     customer_channel,
#     customer_name as company_alias,
#     customer_segment as customer_segment_l3,
#     department as department_alias,
#     customer_group as customer_industry,
#     account_manager,
#     presale,
#     service_name,
#     revenue_type,
#     soc_type,
#     mss_shared_revenue,
#     market_scope,
#     service_code,
#     region,
#     note,
#     current_account_manager,
#     is_vvip_customer,
#     is_master_contract,
#     'VND' as currency_code

# FROM finance_raw.sales_revenue_share_soc_product_category
# """

# # Thực thi và lưu trữ
# df_final = spark.sql(sql_plan)

# df_final.repartition(1).write \
#     .mode("overwrite") \
#     .format("parquet") \
#     .option("path", tgt_cost_path) \
#     .saveAsTable(tgt_cost_table)

# # Refresh metadata
# # spark.sql("REFRESH TABLE " + tgt_cost_table)

# print("--- FINISHED: finance_sales_product_revenue ---")