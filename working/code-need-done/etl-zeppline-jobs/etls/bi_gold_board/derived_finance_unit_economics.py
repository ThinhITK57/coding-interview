%livy.pyspark
spark.catalog.clearCache()

# 1) Config
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

tgt_table = "bi_gold.derived_finance_unit_economics"
tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/derived_finance_unit_economics"

# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

query = """
WITH cost_month AS (
    SELECT
        'MONTH' AS grain_type,
        date_trunc('month', report_date) AS period_start,
        date_trunc('month', report_date) AS period_month,
        date_trunc('quarter', report_date) AS period_quarter,
        date_trunc('year', report_date) AS period_year,
        CAST(NULL AS STRING) AS customer_segment_l1,
        CAST(NULL AS STRING) AS customer_segment_l2,
        CAST(NULL AS STRING) AS customer_segment_l3,
        product_category,
        territory_name,
        business_unit_level_1,
        SUM(base_currency_amount) AS actual_cost_total,
        SUM(CASE WHEN cost_group IN ('Chi phí Nhân công & OS trực tiếp') THEN base_currency_amount ELSE 0 END) AS labor_cogs,
        SUM(CASE WHEN cost_group IN ('Chi phí tài nguyên, CCDC phục vụ sản xuất','Chi phí khấu hao tài sản cố định') THEN base_currency_amount ELSE 0 END) AS infra_cogs,
        SUM(CASE WHEN cost_group IN ('Chi phí phần cứng, phần mềm mua/thuê ngoài') THEN base_currency_amount ELSE 0 END) AS third_party_cogs,
        SUM(CASE WHEN cost_group IN (
            'Chi phí Nhân công & OS trực tiếp',
            'Chi phí tài nguyên, CCDC phục vụ sản xuất',
            'Chi phí phần cứng, phần mềm mua/thuê ngoài',
            'Chi phí khấu hao tài sản cố định'
        ) THEN base_currency_amount ELSE 0 END) AS cogs_amount,
        SUM(CASE WHEN cost_group IN (
            'Chi phí bán hàng',
            'Chi phí quảng cáo truyền thông, tài trợ',
            'Chi phí chăm sóc khách hàng'
        ) THEN base_currency_amount ELSE 0 END) AS acquisition_cost,
        SUM(CASE WHEN cost_group NOT IN (
            'Chi phí Nhân công & OS trực tiếp',
            'Chi phí tài nguyên, CCDC phục vụ sản xuất',
            'Chi phí phần cứng, phần mềm mua/thuê ngoài',
            'Chi phí khấu hao tài sản cố định'
        ) THEN base_currency_amount ELSE 0 END) AS operating_expense_amount,
        SUM(CASE WHEN cost_group NOT IN (
            'Chi phí Nhân công & OS trực tiếp',
            'Chi phí tài nguyên, CCDC phục vụ sản xuất',
            'Chi phí phần cứng, phần mềm mua/thuê ngoài',
            'Chi phí khấu hao tài sản cố định'
        ) THEN base_currency_amount ELSE 0 END) AS operating_expense_excluding_depreciation
    FROM bi_silver.finance_actual_cost
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),
revenue_month AS (
    SELECT
        date_trunc('month', fac_date) AS period_month,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        product_category,
        territory_name,
        SUM(period_value) AS revenue
    FROM bi_silver.crm_contract_allocations
    GROUP BY 1,2,3,4,5,6
),
new_customer_month AS (
    SELECT
        -- Đã sửa: Sử dụng period_month thay vì month_start dựa trên thông báo lỗi
        period_month,
        SUM(new_customer_count) AS new_customer_count
    FROM bi_gold.derived_customer_retention
    GROUP BY 1
),
employee_quarter AS (
    SELECT
        date_trunc('quarter', current_date) AS period_quarter,
        COUNT(DISTINCT CASE WHEN current_status = 'ACTIVE' THEN employee_code END) AS active_employee_count,
        COUNT(DISTINCT CASE WHEN current_status = 'ACTIVE' AND business_unit_level_1 LIKE '%KD%' THEN employee_code END) AS active_sales_employee_count
    FROM bi_silver.hr_employee_onboard
    GROUP BY 1
),
cash_month AS (
    SELECT
        date_trunc('month', report_date) AS period_month,
        SUM(revenue_amount) AS cash_collected_amount
    FROM bi_silver.crm_allocated_revenue
    GROUP BY 1
)
SELECT
    c.grain_type,
    c.period_start,
    c.period_month,
    c.period_quarter,
    c.period_year,
    COALESCE(r.customer_segment_l1, c.customer_segment_l1) AS customer_segment_l1,
    COALESCE(r.customer_segment_l2, c.customer_segment_l2) AS customer_segment_l2,
    COALESCE(r.customer_segment_l3, c.customer_segment_l3) AS customer_segment_l3,
    COALESCE(c.product_category, r.product_category) AS product_category,
    COALESCE(c.territory_name, r.territory_name) AS territory_name,
    c.business_unit_level_1,
    c.actual_cost_total,
    c.labor_cogs,
    c.infra_cogs,
    c.third_party_cogs,
    c.cogs_amount,
    r.revenue,
    (COALESCE(r.revenue, 0) - COALESCE(c.cogs_amount, 0)) AS gross_profit,
    (COALESCE(r.revenue, 0) - COALESCE(c.cogs_amount, 0)) / NULLIF(r.revenue, 0) AS gross_margin,
    c.acquisition_cost,
    n.new_customer_count,
    c.acquisition_cost / NULLIF(n.new_customer_count, 0) AS customer_acquisition_cost,
    e.active_employee_count,
    e.active_sales_employee_count,
    r.revenue / NULLIF(e.active_employee_count, 0) AS revenue_per_active_employee,
    r.revenue / NULLIF(e.active_sales_employee_count, 0) AS revenue_per_active_sales_employee,
    (COALESCE(r.revenue, 0) - COALESCE(c.cogs_amount, 0) - COALESCE(c.acquisition_cost, 0)) AS gross_profit_after_acquisition_cost,
    c.operating_expense_amount,
    c.operating_expense_excluding_depreciation,
    (COALESCE(r.revenue, 0) - COALESCE(c.cogs_amount, 0) - COALESCE(c.operating_expense_amount, 0)) AS operating_profit,
    (COALESCE(r.revenue, 0) - COALESCE(c.cogs_amount, 0) - COALESCE(c.operating_expense_amount, 0)) / NULLIF(r.revenue, 0) AS operating_margin,
    CAST(NULL AS DOUBLE) AS open_pipeline_value,
    CAST(NULL AS BIGINT) AS won_deal_count,
    CAST(NULL AS DOUBLE) AS pipeline_per_active_sales_employee,
    cash.cash_collected_amount,
    r.revenue AS recognized_revenue_amount,
    cash.cash_collected_amount / NULLIF(r.revenue, 0) AS cash_collection_ratio,
    CAST(NULL AS DOUBLE) AS average_collection_days
FROM cost_month c
LEFT JOIN revenue_month r
    ON c.period_month = r.period_month
   AND COALESCE(c.product_category,'') = COALESCE(r.product_category,'')
   AND COALESCE(c.territory_name,'') = COALESCE(r.territory_name,'')
LEFT JOIN new_customer_month n ON c.period_month = n.period_month
LEFT JOIN employee_quarter e ON c.period_quarter = e.period_quarter
LEFT JOIN cash_month cash ON c.period_month = cash.period_month
"""

df_deals = spark.sql(query)

# 3) Write
df_deals.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print("Target Table: " + str(tgt_table))
print("--------------------------------------------------")
print("THANH CONG: Bang da duoc cap nhat.")
print("Tong so dong: " + str(df_deals.count()))
print("--------------------------------------------------")