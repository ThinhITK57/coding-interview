# %livy.pyspark
tgt_table = "bi_silver.crm_mart_revenue_performance_month"
tgt_path  = "s3a://bi-silver/crm_mart_revenue_performance_month"

spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE bi_silver.crm_mart_estimated_revenue")
spark.sql("REFRESH TABLE bi_silver.finance_revenue_plan")
spark.sql("REFRESH TABLE bi_silver.crm_revenue_reconciliation")


query = """
WITH revenue AS (

    SELECT
        CAST(date_trunc('month', report_date) AS DATE) as report_date,
        revenue_segment_l1,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment, 

        SUM(carryover_renew_committed_revenue_amount) AS carryover_renew_committed_revenue_amount,
        SUM(in_year_renew_committed_revenue_amount) AS in_year_renew_committed_revenue_amount,
        SUM(carryover_new_revenue_amount) AS carryover_new_revenue_amount,
        SUM(in_year_new_revenue_amount) AS in_year_new_revenue_amount,
        SUM(committed_non_si_revenue_amount) AS committed_non_si_revenue_amount,
        SUM(committed_si_revenue_amount) AS committed_si_revenue_amount,
        SUM(committed_arr_amount) AS committed_arr_amount,
        SUM(committed_none_arr_amount) AS committed_none_arr_amount,
        SUM(renewal_revenue_amount) AS renewal_revenue_amount,
        SUM(new_revenue_amount) AS new_revenue_amount,
        SUM(other_type_committed_revenue_amount) AS other_type_committed_revenue_amount,
        SUM(committed_revenue_amount) AS committed_revenue_amount,
        SUM(forecast_revenue_amount) AS forecast_revenue_amount,
        SUM(pipeline_revenue_amount) AS pipeline_revenue_amount,
        SUM(expected_non_si_revenue_amount) AS expected_non_si_revenue_amount,
        SUM(expected_si_revenue_amount) AS expected_si_revenue_amount,
        SUM(expected_arr_amount) AS expected_arr_amount,
        SUM(expected_non_arr_amount) AS expected_non_arr_amount,
        SUM(carryover_renew_expected_revenue_amount) AS carryover_renew_expected_revenue_amount,
        SUM(in_year_renew_expected_revenue_amount) AS in_year_renew_expected_revenue_amount,
        SUM(estimated_non_si_revenue_amount) AS estimated_non_si_revenue_amount,
        SUM(estimated_si_revenue_amount) AS estimated_si_revenue_amount,
        SUM(estimated_revenue_amount) AS estimated_revenue_amount,
        SUM(arr_revenue_amount) AS arr_revenue_amount,
        SUM(allocation_revenue_amount) AS allocation_revenue_amount,
        SUM(high_pipeline_revenue_amount) AS high_pipeline_revenue_amount,
        SUM(low_pipeline_revenue_amount) AS low_pipeline_revenue_amount,
        SUM(potential_revenue_amount) AS potential_revenue_amount
        

    FROM bi_silver.crm_mart_estimated_revenue t

    GROUP BY 1,2,3,4,5
),

plan AS (

    SELECT
        CAST(date_trunc('month', CAST(plan_date AS  DATE) ) AS DATE) as report_date,
         -- Customer segments
        revenue_segment_l1,
        
        COALESCE(customer_segment_l1, 'UN') as customer_segment_l1,
        COALESCE(customer_segment_l2, '-')  AS customer_segment_l2,
        CASE
            WHEN customer_segment_l1 = 'Nội bộ'
              OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 in ('Viettel Global Partner' , 'Thị trường') 
              )
               THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN customer_segment_l1 = 'International'  AND customer_segment_l2 in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )  THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'
            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
        END AS customer_segment,
        
        SUM(must_value) AS must_plan_amount,
        SUM(nice_value) AS nice_plan_amount,
        SUM(group_value) AS viettel_group_plan_amount,
        SUM(adjusted_must_value) AS adjusted_must_plan_amount,
        SUM(adjusted_nice_value) AS adjusted_nice_plan_amount,
        SUM(adjusted_group_value) AS adjusted_viettel_group_plan_amount

    FROM bi_silver.crm_revenue_plan_month
    WHERE plan_date is not null

    GROUP BY 1,2,3,4,5
),

actual_tb AS (
    SELECT 
    CAST(date_trunc('month', CAST(invoice_date AS  DATE) ) AS DATE) as report_date,
        COALESCE(revenue_segment_l1, 'UN') as revenue_segment_l1,
        COALESCE(customer_segment_l1, 'UN') as customer_segment_l1,
        COALESCE(customer_segment_l2, '-')  AS customer_segment_l2,
        CASE
            WHEN customer_segment_l1 = 'Nội bộ'
            OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 in ('Viettel Global Partner' , 'Thị trường') 
            )
            THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN customer_segment_l1 = 'International'  AND customer_segment_l2 in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )  THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'
            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
        END AS customer_segment,
        
        SUM(CASE
            WHEN is_hold <> true
            THEN actual_revenue_amount
            ELSE 0.0
        END) AS actual_revenue_amount,
        
        SUM(CASE
            WHEN ( billing_type NOT IN ('Hóa đơn', 'HD', 'HĐ', 'Phân bổ', 'PB')   OR billing_type IS NULL ) AND is_hold = false
            THEN actual_revenue_amount
            ELSE 0.0
        END) AS uninvoiced_revenue_amount
        
        FROM bi_silver.crm_revenue_reconciliation
        WHERE invoice_date is not null

    GROUP BY 1,2,3,4,5
),

unioned AS (

    /* =========================================================
       SOURCE 1: ESTIMATED / COMMITTED / FORECAST REVENUE
       ========================================================= */

    SELECT
        r.report_date AS report_date,
        r.revenue_segment_l1 AS revenue_segment_l1,
        r.customer_segment_l1 AS customer_segment_l1,
        r.customer_segment_l2 AS customer_segment_l2,
        r.customer_segment AS customer_segment,

        CAST(r.carryover_renew_committed_revenue_amount AS DECIMAL(18,2))
            AS carryover_renew_committed_revenue_amount,

        CAST(r.in_year_renew_committed_revenue_amount AS DECIMAL(18,2))
            AS in_year_renew_committed_revenue_amount,

        CAST(r.carryover_new_revenue_amount AS DECIMAL(18,2))
            AS carryover_new_revenue_amount,

        CAST(r.in_year_new_revenue_amount AS DECIMAL(18,2))
            AS in_year_new_revenue_amount,

        CAST(r.committed_non_si_revenue_amount AS DECIMAL(18,2))
            AS committed_non_si_revenue_amount,

        CAST(r.committed_si_revenue_amount AS DECIMAL(18,2))
            AS committed_si_revenue_amount,

        CAST(r.committed_arr_amount AS DECIMAL(18,2))
            AS committed_arr_amount,

        CAST(r.committed_none_arr_amount AS DECIMAL(18,2))
            AS committed_none_arr_amount,

        CAST(r.renewal_revenue_amount AS DECIMAL(18,2))
            AS renewal_revenue_amount,

        CAST(r.new_revenue_amount AS DECIMAL(18,2))
            AS new_revenue_amount,

        CAST(r.other_type_committed_revenue_amount AS DECIMAL(18,2))
            AS other_type_committed_revenue_amount,

        CAST(r.committed_revenue_amount AS DECIMAL(18,2))
            AS committed_revenue_amount,

        CAST(r.forecast_revenue_amount AS DECIMAL(18,2))
            AS forecast_revenue_amount,

        CAST(r.pipeline_revenue_amount AS DECIMAL(18,2))
            AS pipeline_revenue_amount,

        CAST(r.expected_non_si_revenue_amount AS DECIMAL(18,2))
            AS expected_non_si_revenue_amount,

        CAST(r.expected_si_revenue_amount AS DECIMAL(18,2))
            AS expected_si_revenue_amount,

        CAST(r.expected_arr_amount AS DECIMAL(18,2))
            AS expected_arr_amount,

        CAST(r.expected_non_arr_amount AS DECIMAL(18,2))
            AS expected_non_arr_amount,

        CAST(r.carryover_renew_expected_revenue_amount AS DECIMAL(18,2))
            AS carryover_renew_expected_revenue_amount,

        CAST(r.in_year_renew_expected_revenue_amount AS DECIMAL(18,2))
            AS in_year_renew_expected_revenue_amount,

        CAST(r.estimated_non_si_revenue_amount AS DECIMAL(18,2))
            AS estimated_non_si_revenue_amount,

        CAST(r.estimated_si_revenue_amount AS DECIMAL(18,2))
            AS estimated_si_revenue_amount,

        CAST(r.estimated_revenue_amount AS DECIMAL(18,2))
            AS estimated_revenue_amount,

        CAST(r.arr_revenue_amount AS DECIMAL(18,2))
            AS arr_revenue_amount,

        CAST(r.allocation_revenue_amount AS DECIMAL(18,2))
            AS allocation_revenue_amount,

        CAST(r.potential_revenue_amount AS DECIMAL(18,2))
            AS potential_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS actual_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS must_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS nice_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS viettel_group_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS adjusted_must_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS adjusted_nice_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS adjusted_viettel_group_plan_amount
            
        ,CAST(r.high_pipeline_revenue_amount AS DECIMAL(18,2))
                AS high_pipeline_revenue_amount

        ,CAST(r.low_pipeline_revenue_amount AS DECIMAL(18,2))
            AS low_pipeline_revenue_amount

    FROM revenue r


    UNION ALL


    /* =========================================================
       SOURCE 2: REVENUE PLAN
       ========================================================= */

    SELECT
        p.report_date AS report_date,
        p.revenue_segment_l1 AS revenue_segment_l1,
        p.customer_segment_l1 AS customer_segment_l1,
        p.customer_segment_l2 AS customer_segment_l2,
        p.customer_segment AS customer_segment,

        CAST(0 AS DECIMAL(18,2))
            AS carryover_renew_committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS in_year_renew_committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS carryover_new_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS in_year_new_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_non_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_none_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS renewal_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS new_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS other_type_committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS forecast_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS pipeline_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_non_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_non_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS carryover_renew_expected_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS in_year_renew_expected_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS estimated_non_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS estimated_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS estimated_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS arr_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS allocation_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS potential_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS actual_revenue_amount,

        CAST(p.must_plan_amount AS DECIMAL(18,2))
            AS must_plan_amount,

        CAST(p.nice_plan_amount AS DECIMAL(18,2))
            AS nice_plan_amount,

        CAST(p.viettel_group_plan_amount AS DECIMAL(18,2))
            AS viettel_group_plan_amount,

        CAST(p.adjusted_must_plan_amount AS DECIMAL(18,2))
            AS adjusted_must_plan_amount,

        CAST(p.adjusted_nice_plan_amount AS DECIMAL(18,2))
            AS adjusted_nice_plan_amount,

        CAST(p.adjusted_viettel_group_plan_amount AS DECIMAL(18,2))
            AS adjusted_viettel_group_plan_amount

        ,CAST(0 AS DECIMAL(18,2))
            AS high_pipeline_revenue_amount

        ,CAST(0 AS DECIMAL(18,2))
            AS low_pipeline_revenue_amount

    FROM plan p


    UNION ALL


    /* =========================================================
       SOURCE 3: ACTUAL INVOICE REVENUE
       ========================================================= */

    SELECT
        a.report_date AS report_date,
        a.revenue_segment_l1 AS revenue_segment_l1,
        a.customer_segment_l1 AS customer_segment_l1,
        a.customer_segment_l2 AS customer_segment_l2,
        a.customer_segment AS customer_segment,

        CAST(0 AS DECIMAL(18,2))
            AS carryover_renew_committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS in_year_renew_committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS carryover_new_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS in_year_new_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_non_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_none_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS renewal_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS new_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS other_type_committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS committed_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS forecast_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS pipeline_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_non_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS expected_non_arr_amount,

        CAST(0 AS DECIMAL(18,2))
            AS carryover_renew_expected_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS in_year_renew_expected_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS estimated_non_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS estimated_si_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS estimated_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS arr_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS allocation_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS potential_revenue_amount,

        CAST(a.actual_revenue_amount AS DECIMAL(18,2))
            AS actual_revenue_amount,

        CAST(0 AS DECIMAL(18,2))
            AS must_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS nice_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS viettel_group_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS adjusted_must_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS adjusted_nice_plan_amount,

        CAST(0 AS DECIMAL(18,2))
            AS adjusted_viettel_group_plan_amount

        ,CAST(0 AS DECIMAL(18,2))
            AS high_pipeline_revenue_amount

        ,CAST(0 AS DECIMAL(18,2))
            AS low_pipeline_revenue_amount

    FROM actual_tb a
)


SELECT
    u.report_date AS report_date,
    u.revenue_segment_l1 AS revenue_segment_l1,
    u.customer_segment_l1 AS customer_segment_l1,
    u.customer_segment_l2 AS customer_segment_l2,
    u.customer_segment AS report_customer_segment,

    CAST(SUM(u.carryover_renew_committed_revenue_amount) AS DECIMAL(18,2))
        AS carryover_renew_committed_revenue_amount,

    CAST(SUM(u.in_year_renew_committed_revenue_amount) AS DECIMAL(18,2))
        AS in_year_renew_committed_revenue_amount,

    CAST(SUM(u.carryover_new_revenue_amount) AS DECIMAL(18,2))
        AS carryover_new_revenue_amount,

    CAST(SUM(u.in_year_new_revenue_amount) AS DECIMAL(18,2))
        AS in_year_new_revenue_amount,

    CAST(SUM(u.committed_non_si_revenue_amount) AS DECIMAL(18,2))
        AS committed_non_si_revenue_amount,

    CAST(SUM(u.committed_si_revenue_amount) AS DECIMAL(18,2))
        AS committed_si_revenue_amount,

    CAST(SUM(u.committed_arr_amount) AS DECIMAL(18,2))
        AS committed_arr_amount,

    CAST(SUM(u.committed_none_arr_amount) AS DECIMAL(18,2))
        AS committed_none_arr_amount,

    CAST(SUM(u.renewal_revenue_amount) AS DECIMAL(18,2))
        AS renewal_revenue_amount,

    CAST(SUM(u.new_revenue_amount) AS DECIMAL(18,2))
        AS new_revenue_amount,

    CAST(SUM(u.other_type_committed_revenue_amount) AS DECIMAL(18,2))
        AS other_type_committed_revenue_amount,

    CAST(SUM(u.committed_revenue_amount) AS DECIMAL(18,2))
        AS committed_revenue_amount,

    CAST(SUM(u.forecast_revenue_amount) AS DECIMAL(18,2))
        AS forecast_revenue_amount,

    CAST(SUM(u.pipeline_revenue_amount) AS DECIMAL(18,2))
        AS pipeline_revenue_amount,

    CAST(SUM(u.expected_non_si_revenue_amount) AS DECIMAL(18,2))
        AS expected_non_si_revenue_amount,

    CAST(SUM(u.expected_si_revenue_amount) AS DECIMAL(18,2))
        AS expected_si_revenue_amount,

    CAST(SUM(u.expected_arr_amount) AS DECIMAL(18,2))
        AS expected_arr_amount,

    CAST(SUM(u.expected_non_arr_amount) AS DECIMAL(18,2))
        AS expected_non_arr_amount,

    CAST(SUM(u.carryover_renew_expected_revenue_amount) AS DECIMAL(18,2))
        AS carryover_renew_expected_revenue_amount,

    CAST(SUM(u.in_year_renew_expected_revenue_amount) AS DECIMAL(18,2))
        AS in_year_renew_expected_revenue_amount,

    CAST(SUM(u.estimated_non_si_revenue_amount) AS DECIMAL(18,2))
        AS estimated_non_si_revenue_amount,

    CAST(SUM(u.estimated_si_revenue_amount) AS DECIMAL(18,2))
        AS estimated_si_revenue_amount,

    CAST(SUM(u.estimated_revenue_amount) AS DECIMAL(18,2))
        AS estimated_revenue_amount,

    CAST(SUM(u.arr_revenue_amount) AS DECIMAL(18,2))
        AS arr_revenue_amount,

    CAST(SUM(u.allocation_revenue_amount) AS DECIMAL(18,2))
        AS allocation_revenue_amount,

    CAST(SUM(u.potential_revenue_amount) AS DECIMAL(18,2))
        AS potential_revenue_amount,

    CAST(SUM(u.actual_revenue_amount) AS DECIMAL(18,2))
        AS actual_revenue_amount,

    CAST(SUM(u.high_pipeline_revenue_amount) AS DECIMAL(18,2))
        AS high_pipeline_revenue_amount,

    CAST(SUM(u.low_pipeline_revenue_amount) AS DECIMAL(18,2))
        AS low_pipeline_revenue_amount,

    CAST(SUM(u.must_plan_amount) AS DECIMAL(18,2))
        AS must_plan_amount,

    CAST(SUM(u.nice_plan_amount) AS DECIMAL(18,2))
        AS nice_plan_amount,

    CAST(SUM(u.viettel_group_plan_amount) AS DECIMAL(18,2))
        AS viettel_group_plan_amount,

    CAST(SUM(u.adjusted_must_plan_amount) AS DECIMAL(18,2))
        AS adjusted_must_plan_amount,

    CAST(SUM(u.adjusted_nice_plan_amount) AS DECIMAL(18,2))
        AS adjusted_nice_plan_amount,

    CAST(SUM(u.adjusted_viettel_group_plan_amount) AS DECIMAL(18,2))
        AS adjusted_viettel_group_plan_amount

FROM unioned u

GROUP BY
    u.report_date,
    u.revenue_segment_l1,
    u.customer_segment_l1,
    u.customer_segment_l2,
    u.customer_segment

ORDER BY
    u.report_date,
    u.revenue_segment_l1,
    u.customer_segment_l1,
    u.customer_segment_l2,
    u.customer_segment
"""

df = spark.sql(query)


# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)
#   .save(tgt_path)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

