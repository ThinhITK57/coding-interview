# %livy.pyspark


# OLD: biz_production_cost_allocation
# NEW: finance_production_cost_allocations

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_production_cost_allocation")
# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_production_cost_allocations")

# 1) Config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# =========================
# FILE 2: ACTUAL_COST
# =========================
src_actual_table = "finance_raw.production_cost_allocation"
tgt_actual_table = "bi_silver.finance_production_cost_allocations"
tgt_actual_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_production_cost_allocations"

# Đọc từ bảng
df_actual = spark.table(src_actual_table)
df_actual.createOrReplaceTempView("actual_cost_view")

sql_actual = """
    SELECT 
        -- 1) Xử lý thời gian từ date_key (2025-01-31 00:00:00)
        TO_TIMESTAMP(TO_DATE(report_date)) AS report_date, 
        YEAR(report_date) as report_year,
        MONTH(report_date) as report_month,
        
        -- 2) Mapping thông tin sản phẩm/dịch vụ
        CAST(old_category AS STRING) AS old_category,
        CAST(old_category AS STRING) AS old_product_category,
        CAST(category_code AS STRING) AS product_category_code,
        CAST(km_cost_code AS STRING) AS cost_group,
        
        -- 4) Mapping tiền tệ và vùng miền
        CAST(base_currency_amount AS DECIMAL(18,2)) * 1000000 AS base_currency_amount,
        'VND' AS currency_code,
        CAST(territory_name AS STRING) AS territory_name,
        
        km_cost_code AS expense_code,
        territory_name as market_name

    FROM actual_cost_view
"""

# Thực thi ghi đè và gom file
df_final = spark.sql(sql_actual)

df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_actual_path) \
    .saveAsTable(tgt_actual_table)

spark.sql("REFRESH TABLE " + tgt_actual_table)
print(tgt_actual_table)

print("--- FINISHED: ACTUAL_COST ---")
# Kiểm tra 5 dòng dữ liệu cuối cùng