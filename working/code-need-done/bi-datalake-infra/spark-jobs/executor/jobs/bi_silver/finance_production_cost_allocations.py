# %livy.pyspark

tgt_actual_table = "bi_silver.finance_production_cost_allocations"
tgt_actual_path  = "s3a://bi-silver/finance_production_cost_allocations"

# 1) Config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# =========================
# FILE 2: ACTUAL_COST
# =========================

sql_actual = """
SELECT
    -- Thời gian
    TO_TIMESTAMP(TO_DATE(date_key)) AS report_date,
    
    -- Thông tin sản phẩm
    CAST(product_service_name AS STRING) AS product_service_name,
    CAST(product_service_code AS STRING) AS product_service_code,
    CAST(product_service_expense_category AS STRING) AS product_service_expense_category,

    -- Nhóm chi phí
    CAST(expense_code AS STRING) AS expense_code,

    -- Loại dữ liệu
    CAST(data_type AS STRING) AS data_type,

    -- Thị trường
    CAST(market AS STRING) AS market_name,

    -- Giá trị (đơn vị trong source là triệu VND)
    CAST(amount_million_vnd AS DECIMAL(18,2)) AS amount_vnd,
    'VND' AS currency_code

FROM finance_raw.production_cost_allocation
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