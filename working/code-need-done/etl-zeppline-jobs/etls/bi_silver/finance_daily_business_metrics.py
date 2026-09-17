%livy.pyspark

target_table = "bi_silver.finance_daily_business_metrics"
target_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_daily_business_metrics"

# spark.sql("DROP VIEW IF EXISTS bi_silver.finance_daily_business_metrics")

df_final = spark.sql("""
WITH actual_cost AS (

    SELECT
        CAST(report_date AS DATE) AS report_date,
        CASE WHEN product_category is null or product_category = '' THEN 'Khác' ELSE product_category END AS product_category,
        CASE WHEN business_unit_level_1 is null or business_unit_level_1 = '' THEN 'Khác' ELSE business_unit_level_1 END AS business_unit_level_1,
        CASE WHEN territory_name is null or territory_name = '' THEN 'Khác' ELSE territory_name END AS territory_name,
        CAST(SUM(base_currency_amount) AS DECIMAL(18,2)) AS actual_cost_amount
    FROM bi_silver.finance_actual_cost
    GROUP BY 1,2,3,4

),

sales_revenue AS (

    SELECT
        CAST(t1.invoice_date AS DATE) AS report_date,
        COALESCE(t2.product_category, t1.service_name) AS product_category,
        COALESCE(NULLIF(t1.department,''), 'Khác') AS business_unit_level_1,
        COALESCE(NULLIF(t1.region,''), 'Khác') AS territory_name,
        CAST(SUM(t1.final_revenue_amount) AS DECIMAL(18,2)) AS sales_revenue_amount
    FROM finance_raw.sales_revenue t1
    LEFT JOIN finance_raw.dim_product_category_code t2
        ON t1.service_code = t2.category_code
    GROUP BY 1,2,3,4

),

revenue_plan AS (

    SELECT
        CAST(plan_date AS DATE) AS report_date,
        CASE WHEN product_category is null or product_category = '' THEN 'Khác' ELSE product_category END AS product_category,
        CAST(SUM(plan_must_amount) AS DECIMAL(18,2)) AS planned_revenue_must_amount,
        CAST(SUM(plan_should_amount) AS DECIMAL(18,2)) AS planned_revenue_should_amount,
        CAST(SUM(plan_nice_amount) AS DECIMAL(18,2)) AS planned_revenue_nice_amount
    FROM bi_silver.finance_product_revenue_plan
    GROUP BY 1,2

),

fact_union AS (

    SELECT
        report_date,
        product_category,
        business_unit_level_1,
        territory_name,
        actual_cost_amount,
        CAST(0 AS DECIMAL(18,2)) AS sales_revenue_amount,
        CAST(0 AS DECIMAL(18,2)) AS planned_revenue_must_amount,
        CAST(0 AS DECIMAL(18,2)) AS planned_revenue_should_amount,
        CAST(0 AS DECIMAL(18,2)) AS planned_revenue_nice_amount
    FROM actual_cost

    UNION ALL

    SELECT
        report_date,
        product_category,
        business_unit_level_1,
        territory_name,
        CAST(0 AS DECIMAL(18,2)),
        sales_revenue_amount,
        CAST(0 AS DECIMAL(18,2)),
        CAST(0 AS DECIMAL(18,2)),
        CAST(0 AS DECIMAL(18,2))
    FROM sales_revenue

    UNION ALL

    SELECT
        report_date,
        product_category,
        NULL AS business_unit_level_1,
        NULL AS territory_name,
        CAST(0 AS DECIMAL(18,2)),
        CAST(0 AS DECIMAL(18,2)),
        planned_revenue_must_amount,
        planned_revenue_should_amount,
        planned_revenue_nice_amount
    FROM revenue_plan

)

SELECT

    CAST(report_date AS TIMESTAMP) AS report_date,

    COALESCE(product_category,'Khác') AS product_category,
    COALESCE(business_unit_level_1,'Khác') AS business_unit_level_1,
    COALESCE(territory_name,'Khác') AS territory_name,

    CAST(SUM(actual_cost_amount) AS DECIMAL(18,2)) AS actual_cost_amount,
    CAST(SUM(sales_revenue_amount) AS DECIMAL(18,2)) AS sales_revenue_amount,
    CAST(SUM(planned_revenue_must_amount) AS DECIMAL(18,2)) AS planned_revenue_must_amount,
    CAST(SUM(planned_revenue_should_amount) AS DECIMAL(18,2)) AS planned_revenue_should_amount,
    CAST(SUM(planned_revenue_nice_amount) AS DECIMAL(18,2)) AS planned_revenue_nice_amount,

    CAST(
        SUM(sales_revenue_amount) - SUM(actual_cost_amount)
        AS DECIMAL(18,2)
    ) AS gross_profit_amount,

    CAST(
        SUM(sales_revenue_amount) - SUM(planned_revenue_must_amount)
        AS DECIMAL(18,2)
    ) AS revenue_vs_plan_gap_amount

FROM fact_union

GROUP BY
    report_date,
    COALESCE(product_category,'Khác'),
    COALESCE(business_unit_level_1,'Khác'),
    COALESCE(territory_name,'Khác')

""")

# Thực thi và lưu trữ
df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)

#  .save(target_path)
# .option("path", target_path) \
    # .saveAsTable(target_table)
    
# Refresh metadata
spark.sql("REFRESH TABLE " + target_table)