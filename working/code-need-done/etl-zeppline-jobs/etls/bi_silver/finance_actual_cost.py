# %livy.pyspark


# OLD: biz_actual_cost
# NEW: finance_actual_cost

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_actual_cost")
# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_actual_cost")
spark.catalog.refreshTable("finance_raw.actual_cost")

# --- XỬ LÝ FILE 2: ACTUAL_COST ---
# Đọc dữ liệu từ nguồn Parquet
# Xóa bảng cũ nếu tồn tại

sql_cost = """
    SELECT 
        CAST(TO_DATE(report_date) as TIMESTAMP) as report_date, 
        YEAR(report_date) as report_year, 
        MONTH(report_date) as report_month, 
        CAST(old_category AS STRING) as old_category,
        CAST(old_category AS STRING) as old_category,
        CAST(category_code AS STRING) as product_category_code,
        CAST(cost_group AS STRING) as cost_group,
        CAST(base_currency_amount AS DECIMAL(18,2)) as base_currency_amount,
        currency_code as currency_code,
        CAST(territory_name AS STRING) as territory_name,
        CAST(product_category AS STRING) as product_category,
        CAST(unit_level_1 AS STRING) as business_unit_level_1
    FROM finance_raw.actual_cost
"""


# Thực thi và lưu trữ
df_final = spark.sql(sql_cost)

df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", '/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_actual_cost') \
    .saveAsTable("bi_silver.finance_actual_cost")
    
# --- KIỂM TRA KẾT QUẢ ---
print("Thống kê số lượng bản ghi sau khi xử lý:")
spark.catalog.refreshTable("bi_silver.finance_actual_cost")
