%livy.pyspark


# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_fact_ratios")

# 1) Config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.catalog.clearCache()

src_cost_table = "finance_raw.financial_ratio"
tgt_cost_table = "bi_silver.finance_fact_ratios"
tgt_cost_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_fact_ratios"

# Đọc từ bảng nguồn
df_plan = spark.table(src_cost_table)
df_plan.createOrReplaceTempView("financial_ratio")

sql_plan = """
    SELECT 
        TO_TIMESTAMP(TO_DATE(report_date)) as report_date,
         CASE
            WHEN description = 'Hệ số thanh toán hiện thời' THEN 'CURRENT_RATIO'
            WHEN description = 'Hệ số nợ trên vốn chủ sở hữu (D/E)' THEN 'DEBT_TO_EQUITY'
            WHEN description = 'ROE' THEN 'ROE'
            ELSE 'UNKNOWN'
        END AS metric_code,
        description as metric_name,
        CASE
            WHEN value IS NULL THEN NULL
            WHEN CAST(value AS STRING) = 'NaN' THEN NULL
            ELSE ROUND(CAST(value AS DOUBLE), 4)
        END AS metric_value
        
    FROM financial_ratio
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

print("--- FINISHED: finance_fact_ratios ---")