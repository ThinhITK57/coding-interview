# %livy.pyspark

target_table = "bi_silver.finance_metrics"
target_path  = "s3a://bi-silver/finance_metrics"

# spark.sql("DROP VIEW IF EXISTS bi_silver.finance_metrics")

df_final  = spark.sql("""
WITH rev_agg AS (
    SELECT
        CAST(t1.invoice_date AS DATE) AS report_date,
        COALESCE(t2.product_category, t1.service_name) AS product_category,
        COALESCE(NULLIF(t1.department, ''), 'Khác') AS business_unit_level_1,

        CAST(SUM(t1.final_revenue_amount) AS DECIMAL(18,2)) AS total_revenue,

        CAST(0 AS DECIMAL(18,2)) AS fixed_asset_depreciation_cost,
        CAST(0 AS DECIMAL(18,2)) AS direct_labor_os_cost,
        CAST(0 AS DECIMAL(18,2)) AS production_infra_cost,
        CAST(0 AS DECIMAL(18,2)) AS business_os_cost,
        CAST(0 AS DECIMAL(18,2)) AS total_cost

    FROM finance_raw.sales_revenue t1
    LEFT JOIN finance_raw.dim_product_category_code t2
        ON t1.service_code = t2.category_code

    GROUP BY
        CAST(t1.invoice_date AS DATE),
        COALESCE(NULLIF(t1.service_code, ''), 'Khác'),
        COALESCE(t2.product_category, t1.service_name),
        COALESCE(NULLIF(t1.department, ''), 'Khác')
),

cost_agg AS (
    SELECT
        CAST(report_date AS DATE) AS report_date,
        product_category,
        business_unit_level_1,

        CAST(0 AS DECIMAL(18,2)) AS total_revenue,

        CAST(SUM(CASE
                WHEN cost_group = 'Chi phí khấu hao tài sản cố định'
                THEN base_currency_amount ELSE 0
            END) AS DECIMAL(18,2)) AS fixed_asset_depreciation_cost,

        CAST(SUM(CASE
                WHEN cost_group IN (
                    'Chi phí Nhân công & OS trực tiếp',
                    'Chi phí Nhân công, outsource trực tiếp'
                )
                THEN base_currency_amount ELSE 0
            END) AS DECIMAL(18,2)) AS direct_labor_os_cost,

        CAST(SUM(CASE
                WHEN cost_group IN (
                    'Chi phí tài nguyên, CCDC phục vụ sản xuất',
                    'Chi phí hạ tầng, máy chủ, CCDC sản xuất'
                )
                THEN base_currency_amount ELSE 0
            END) AS DECIMAL(18,2)) AS production_infra_cost,

        CAST(SUM(CASE
                WHEN cost_group = 'Chi phí OS kinh doanh'
                THEN base_currency_amount ELSE 0
            END) AS DECIMAL(18,2)) AS business_os_cost,

        CAST(SUM(base_currency_amount) AS DECIMAL(18,2)) AS total_cost

    FROM bi_silver.finance_actual_cost

    GROUP BY
        CAST(report_date AS DATE),
        product_category,
        business_unit_level_1
),

union_all_join AS (

    SELECT * FROM rev_agg

    UNION ALL

    SELECT * FROM cost_agg
),

final_join AS (

    SELECT
        report_date,
        product_category,
        business_unit_level_1,

        SUM(total_revenue) AS total_revenue,
        SUM(total_cost) AS total_cost,
        SUM(direct_labor_os_cost) AS direct_labor_os_cost,
        SUM(production_infra_cost) AS production_infra_cost,
        SUM(fixed_asset_depreciation_cost) AS fixed_asset_depreciation_cost,
        SUM(business_os_cost) AS business_os_cost

    FROM union_all_join

    GROUP BY
        report_date,
        product_category,
        business_unit_level_1
)

SELECT
    CAST(report_date AS TIMESTAMP) AS report_date,
    product_category AS product_category,
    business_unit_level_1 AS business_unit_level_1,

    CAST( total_revenue AS DECIMAL(18,2) ) AS total_revenue,
    CAST( total_cost AS DECIMAL(18,2) ) AS total_cost,

    CAST( total_revenue - direct_labor_os_cost - production_infra_cost AS DECIMAL(18,2) ) AS gross_profit,
    CAST( total_revenue - total_cost + fixed_asset_depreciation_cost  AS DECIMAL(18,2) ) AS ebitda,

    CAST( fixed_asset_depreciation_cost AS DECIMAL(18,2) ) AS  fixed_asset_depreciation_cost,
    CAST( direct_labor_os_cost AS DECIMAL(18,2) ) AS  direct_labor_os_cost,
    CAST( production_infra_cost AS DECIMAL(18,2) ) AS  production_infra_cost,
    CAST( business_os_cost AS DECIMAL(18,2) ) AS  business_os_cost

FROM final_join

""")

# Thực thi và lưu trữ
df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)
    
# .save(target_path)
#  .option("path", target_path) \
    # .saveAsTable(target_table)
    
# Refresh metadata
spark.sql("REFRESH TABLE " + target_table)