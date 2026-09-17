# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_contract_collected_invoices")

# 1) Config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.catalog.clearCache()

target_table = "bi_silver.finance_contract_collected_invoices"
target_path  = "s3a://bi-silver/finance_contract_collected_invoices"

sql_query = """
WITH src AS (
    SELECT
        *,
        NULLIF(TRIM(customer_name), '') AS clean_customer_name,

        CASE
            WHEN overdue_months IN (
                'Chưa quá hạn',
                'Đã thanh toán',
                'Công nợ Đã thu + đã bù trừ công nợ'
            ) THEN 0D
            ELSE TRY_CAST(overdue_months AS DOUBLE)
        END AS overdue_months_numeric_clean
    FROM finance_raw.contract_collected_invoices
    WHERE NULLIF(TRIM(customer_name), '') IS NOT NULL
)

SELECT
    stt AS idd,
    company AS sales_channel,
    transaction_code,
    invoice_number,
    contract_order_number AS contract_num,
    pmtc_code AS customer_code,
    description AS contract_description,
    clean_customer_name AS company_name,

    CAST(original_currency AS DECIMAL(18,3)) AS original_amount,
    CAST(initial_receivable AS DECIMAL(18,3)) AS initial_receivable_amount,
    CAST(collected AS DECIMAL(18,3)) AS total_received_amount,
    CAST(outstanding_receivable AS DECIMAL(18,3)) AS remaining_balance_amount,

    TO_TIMESTAMP(TO_DATE(invoice_date)) AS invoice_date,
    TO_TIMESTAMP(TO_DATE(payment_deadline)) AS payment_deadline,
    TO_TIMESTAMP(TO_DATE(payment_date)) AS payment_date,

    overdue_months_numeric_clean AS overdue_months_numeric,

    CASE
        WHEN overdue_months IN (
            'Công nợ Đã thu + đã bù trừ công nợ',
            'Đã thanh toán'
        )
            THEN '6. Công nợ Đã thu + đã bù trừ công nợ'

        WHEN overdue_months = 'Chưa quá hạn'
            THEN '0. Chưa quá hạn'

        WHEN overdue_months_numeric_clean > 0
         AND overdue_months_numeric_clean <= 1
            THEN '1. Chậm < 1 tháng'

        WHEN overdue_months_numeric_clean > 1
         AND overdue_months_numeric_clean <= 3
            THEN '2. Chậm < 3 tháng'

        WHEN overdue_months_numeric_clean > 3
         AND overdue_months_numeric_clean <= 6
            THEN '3. Chậm 3 tháng - 6 tháng'

        WHEN overdue_months_numeric_clean > 6
         AND overdue_months_numeric_clean <= 12
            THEN '4. Chậm 6 tháng - 1 năm'

        WHEN overdue_months_numeric_clean > 12
            THEN '5. Chậm trên 1 năm'

        ELSE '99. Không xác định'
    END AS overdue_status_enum,

    CASE
        WHEN account_manager LIKE '%@%'
            THEN REGEXP_EXTRACT(account_manager, '^(.*?)@', 1)
        ELSE account_manager
    END AS am_username

FROM src
"""

df_final = spark.sql(sql_query)

df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .saveAsTable(target_table)

print("--- FINISHED: finance_contract_collected_invoices ---")