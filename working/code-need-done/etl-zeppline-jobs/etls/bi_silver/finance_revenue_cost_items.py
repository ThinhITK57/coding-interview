# %livy.pyspark

target_table = "bi_silver.finance_revenue_cost_items"
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_revenue_cost_items"

df_final = spark.sql("""
SELECT
    CAST(TO_DATE(report_date) AS TIMESTAMP) AS report_date,
    CAST(data_type AS STRING) AS item_type_code,

    CASE
        WHEN data_type = 'DT' THEN 'Doanh thu'
        WHEN data_type = 'CP' THEN 'Chi phí'
        ELSE 'Không xác định'
    END AS item_type_name,

    CAST(expense_category_level_2 AS STRING) AS item_category_level_2,
    CAST(expense_code AS STRING) AS item_code,

    CAST(amount_million_vnd AS DECIMAL(18,2)) AS base_currency_amount,
    'VND' as currency_code

FROM finance_raw.cost_items
""")

df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)

    # .option("path", target_path) \
    # .saveAsTable(target_table)
    # .save(target_path)
    
spark.sql(f"REFRESH TABLE {target_table}")

print("Đã tạo mới hoặc overwrite bảng:", target_table)
