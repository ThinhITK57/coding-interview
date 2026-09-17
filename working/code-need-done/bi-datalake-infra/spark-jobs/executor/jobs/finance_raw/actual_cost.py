# # %livy.pyspark


# # spark.sql("DROP TABLE IF EXISTS finance_raw.actual_cost")

# # --- XỬ LÝ FILE 2: ACTUAL_COST ---
# # Đọc dữ liệu từ nguồn Parquet
# df_spdv = spark.read.parquet('/opt/datasets/DWS/VCS_Sales/trino_chiphi/actual_cost2025.parquet')
# df_spdv.createOrReplaceTempView("actual_cost_view")
# spark.sql("""
#     CREATE TABLE finance_raw.actual_cost
#     USING PARQUET
#     LOCATION 's3a://vcs-raw/finance-raw/actual_cost'
#     AS
#     SELECT 
#         TO_DATE(report_date) as report_date, 
#         YEAR(report_date) as report_year, 
#         MONTH(report_date) as report_month, 
#         CAST(old_category AS STRING) as old_category,
#         CAST(category_code AS STRING) as category_code,
#         CAST(cost_group AS STRING) as cost_group,
#         CAST(actual_cost AS DECIMAL(18,2)) as base_currency_amount,
#         'VND' as currency_code,
#         CAST(territory AS STRING) as territory_name,
#         CAST(category AS STRING) as product_category,
#         CAST(department AS STRING) as unit_level_1,
#         '/opt/datasets/DWS/VCS_Sales/trino_chiphi/actual_cost2025.parquet' as source_file

  
#     FROM actual_cost_view
# """)

# # Hiển thị 5 dòng đầu tiên để kiểm tra định dạng ngày tháng và số tiền
# spark.sql("SELECT * FROM finance_raw.actual_cost LIMIT 5").show()