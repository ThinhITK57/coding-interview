%livy.pyspark
spark.catalog.clearCache()

# 1) Config & refresh
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE crm_raw.deals")
spark.sql("REFRESH TABLE crm_raw.deleted_deals")
spark.sql("REFRESH TABLE crm_raw.deal_stages")
spark.sql("REFRESH TABLE crm_raw.cm_contracts")
spark.sql("REFRESH TABLE crm_raw.deal_payment_statuses")
spark.sql("REFRESH TABLE crm_raw.currencies")
spark.sql("REFRESH TABLE crm_raw.deal_reasons")

tgt_table = "bi_gold.derived_growth_target_pipeline"
tgt_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/derived_growth_target_pipeline"

query = """
CREATE OR REPLACE TABLE bi_gold.derived_growth_target_pipeline AS
WITH target_month AS (
    SELECT
        'MONTH' AS grain_type,
        CAST(date_trunc('month', plan_date) AS date) AS period_start,
        CAST(date_trunc('month', plan_date) AS date) AS period_month,
        CAST(date_trunc('quarter', plan_date) AS date) AS period_quarter,
        CAST(date_trunc('year', plan_date) AS date) AS period_year,
        customer_segment_l1,
        customer_segment_l2,
        '' AS customer_segment_l3,
        '' AS product_group,
        '' AS product_name,
        '' AS product_category,
        '' AS territory_name,
        '' AS deal_stage_name,
        SUM(plan_must) AS target_revenue,
        SUM(plan_nice) AS stretch_target_revenue,
        SUM(plan_viettel_group) AS viettel_group_target_revenue
    FROM bi_silver.finance_revenue_plan
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),
target_product_month AS (
    SELECT
        'MONTH' AS grain_type,
        CAST(date_trunc('month', plan_date) AS date) AS period_start,
        CAST(date_trunc('month', plan_date) AS date) AS period_month,
        CAST(date_trunc('quarter', plan_date) AS date) AS period_quarter,
        CAST(date_trunc('year', plan_date) AS date) AS period_year,
        '' AS customer_segment_l1,
        '' AS customer_segment_l2,
        '' AS customer_segment_l3,
        product_group,
        product_name,
        '' AS product_category,
        '' AS territory_name,
        '' AS deal_stage_name,
        SUM(plan_must) AS target_revenue,
        SUM(plan_nice) AS stretch_target_revenue,
        0 AS viettel_group_target_revenue
    FROM bi_silver.finance_product_revenue_plan
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),
pipeline_month AS (
    SELECT
        'MONTH' AS grain_type,
        CAST(date_trunc('month', created_at) AS date) AS period_start,
        CAST(date_trunc('month', created_at) AS date) AS period_month,
        CAST(date_trunc('quarter', created_at) AS date) AS period_quarter,
        CAST(date_trunc('year', created_at) AS date) AS period_year,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        '' AS product_group,
        '' AS product_name,
        '' AS product_category,
        territory_name,
        deal_stage_name,
        SUM(CASE WHEN deal_status NOT IN ('Signed / Ký hợp đồng','Lost') THEN expected_deal_value ELSE 0 END) AS open_pipeline_raw_value,
        SUM(CASE WHEN deal_status NOT IN ('Signed / Ký hợp đồng','Lost') THEN expected_deal_value * probability / 100.0 ELSE 0 END) AS open_pipeline_weighted_value
    FROM bi_silver.crm_deals
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),
revenue_month AS (
    SELECT
        'MONTH' AS grain_type,
        CAST(date_trunc('month', fac_date) AS date) AS period_start,
        CAST(date_trunc('month', fac_date) AS date) AS period_month,
        CAST(date_trunc('quarter', fac_date) AS date) AS period_quarter,
        CAST(date_trunc('year', fac_date) AS date) AS period_year,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        '' AS product_group,
        '' AS product_name,
        product_category,
        territory_name,
        '' AS deal_stage_name,
        SUM(period_value) AS actual_revenue
    FROM bi_silver.crm_contract_allocations
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),
base AS (
    SELECT
        COALESCE(t.grain_type, p.grain_type, r.grain_type, tp.grain_type) AS grain_type,
        COALESCE(t.period_start, p.period_start, r.period_start, tp.period_start) AS period_start,
        COALESCE(t.period_month, p.period_month, r.period_month, tp.period_month) AS period_month,
        COALESCE(t.period_quarter, p.period_quarter, r.period_quarter, tp.period_quarter) AS period_quarter,
        COALESCE(t.period_year, p.period_year, r.period_year, tp.period_year) AS period_year,
        COALESCE(t.customer_segment_l1, p.customer_segment_l1, r.customer_segment_l1, '') AS customer_segment_l1,
        COALESCE(t.customer_segment_l2, p.customer_segment_l2, r.customer_segment_l2, '') AS customer_segment_l2,
        COALESCE(p.customer_segment_l3, r.customer_segment_l3, '') AS customer_segment_l3,
        COALESCE(t.product_group, tp.product_group, '') AS product_group,
        COALESCE(t.product_name, tp.product_name, '') AS product_name,
        COALESCE(r.product_category, tp.product_category, '') AS product_category,
        COALESCE(p.territory_name, r.territory_name, '') AS territory_name,
        COALESCE(p.deal_stage_name, '') AS deal_stage_name,
        COALESCE(t.target_revenue, tp.target_revenue, 0) AS target_revenue,
        COALESCE(t.stretch_target_revenue, tp.stretch_target_revenue, 0) AS stretch_target_revenue,
        COALESCE(t.viettel_group_target_revenue, 0) AS viettel_group_target_revenue,
        COALESCE(p.open_pipeline_raw_value, 0) AS open_pipeline_raw_value,
        COALESCE(p.open_pipeline_weighted_value, 0) AS open_pipeline_weighted_value,
        COALESCE(r.actual_revenue, 0) AS actual_revenue
    FROM target_month t
    FULL OUTER JOIN pipeline_month p
        ON t.period_month = p.period_month 
       AND COALESCE(t.customer_segment_l1,'') = COALESCE(p.customer_segment_l1,'')
       AND COALESCE(t.customer_segment_l2,'') = COALESCE(p.customer_segment_l2,'')
    FULL OUTER JOIN revenue_month r
        ON COALESCE(t.period_month, p.period_month) = r.period_month
       AND COALESCE(t.customer_segment_l1, p.customer_segment_l1, '') = COALESCE(r.customer_segment_l1,'')
       AND COALESCE(t.customer_segment_l2, p.customer_segment_l2, '') = COALESCE(r.customer_segment_l2,'')
       AND COALESCE(p.customer_segment_l3,'') = COALESCE(r.customer_segment_l3,'')
    FULL OUTER JOIN target_product_month tp
        ON COALESCE(t.period_month, p.period_month, r.period_month) = tp.period_month
)
SELECT
    *,
    LAG(actual_revenue, 1) OVER (
        PARTITION BY grain_type, customer_segment_l1, customer_segment_l2, customer_segment_l3, product_category, territory_name
        ORDER BY period_start
    ) AS previous_period_revenue,
    open_pipeline_raw_value / NULLIF(target_revenue, 0) AS pipeline_coverage_raw,
    open_pipeline_weighted_value / NULLIF(target_revenue, 0) AS pipeline_coverage_weighted,
    actual_revenue / NULLIF(target_revenue, 0) AS revenue_achievement_rate,
    (actual_revenue - target_revenue) AS revenue_gap_to_target
FROM base
"""

df_deals = spark.sql(query)

# # 2) Drop đúng table + hard delete path chống double


# 3) Write
df_deals.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)


spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("--------------------------------------------------")
print("THANH CONG: Bang  da duoc cap nhat.")
print("Tong so deals (unique id): " + str(df_deals.count()))
print("--------------------------------------------------")
