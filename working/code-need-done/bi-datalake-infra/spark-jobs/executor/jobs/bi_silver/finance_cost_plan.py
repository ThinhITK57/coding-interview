# %livy.pyspark


# OLD: biz_cost_plan
# NEW: finance_cost_plan

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_cost_plan")
# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_cost_plan")
spark.sql("REFRESH TABLE finance_raw.cost_plan ")


# 1) Config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.catalog.clearCache()

# =========================
# FILE 1: PLAN_COST
# =========================
src_cost_table = "finance_raw.cost_plan"
tgt_cost_table = "bi_silver.finance_cost_plan"
tgt_cost_path  = "s3a://bi-silver/finance_cost_plan"

# Đọc từ bảng nguồn
df_plan = spark.table(src_cost_table)
df_plan.createOrReplaceTempView("raw_cost")

sql_plan = """
    SELECT 
        -- 1) Chuyển đổi date_key (2025-01-31 00:00:00) sang Date
        TO_TIMESTAMP(TO_DATE(date_key)) as plan_date,
        YEAR(date_key) as plan_year,
        MONTH(date_key) as plan_month,
        
        -- 2) Lấy năm từ date_key
        -- YEAR(TO_DATE(date_key)) AS plan_year,
        
        -- 3) Lấy tháng từ date_key
        -- MONTH(TO_DATE(date_key)) as plan_month,
        
        -- 4) Mapping các cột theo schema nguồn
        CAST(expense_code AS STRING) as cost_code,
        CAST(expense_category_level_2 AS STRING) as cost_group,
        
        -- 5) Chuyển amount sang Decimal (đảm bảo độ chính xác tiền tệ)
        CAST(amount_million_vnd AS DECIMAL(18,2)) as expected_cost_value,
        
        'VND' as currency_code
        
    FROM raw_cost
"""

# Thực thi và lưu trữ
df_final = spark.sql(sql_plan)

df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_cost_path) \
    .saveAsTable(tgt_cost_table)

# Refresh metadata
spark.sql("REFRESH TABLE " + tgt_cost_table)

print("--- FINISHED: PLAN_COST ---")
# Kiểm tra nhanh kết quả
print(tgt_cost_table)