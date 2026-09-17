%livy.pyspark
spark.catalog.clearCache()

# 1) Config
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

tgt_table = "bi_gold.derived_sales_execution"
tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/derived_sales_execution"

# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

query = """
WITH base AS (
    SELECT
        'MONTH' AS grain_type,
        date_trunc('month', created_at) AS period_start,
        date_trunc('month', created_at) AS period_month,
        date_trunc('quarter', created_at) AS period_quarter,
        date_trunc('year', created_at) AS period_year,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        territory_name,
        channel,
        deal_stage_name,
        COUNT(DISTINCT CASE WHEN deal_status IN ('Signed / Ký hợp đồng','Lost') THEN deal_id END) AS closed_deal_count,
        COUNT(DISTINCT CASE WHEN deal_status = 'Signed / Ký hợp đồng' THEN deal_id END) AS won_deal_count,
        COUNT(DISTINCT CASE WHEN deal_status = 'Lost' THEN deal_id END) AS lost_deal_count,
        SUM(CASE WHEN deal_status = 'Signed / Ký hợp đồng' THEN vnd_amount ELSE 0 END) AS won_deal_value,
        SUM(CASE WHEN deal_status = 'Signed / Ký hợp đồng' AND created_at IS NOT NULL AND sign_date IS NOT NULL
            THEN datediff(sign_date, created_at) ELSE 0 END) AS total_sales_cycle_days,
        COUNT(DISTINCT CASE WHEN deal_status = 'Signed / Ký hợp đồng' AND created_at IS NOT NULL AND sign_date IS NOT NULL
            THEN deal_id END) AS won_deals_with_valid_cycle,
        approx_percentile(CASE WHEN deal_status = 'Signed / Ký hợp đồng' THEN vnd_amount END, 0.5) AS median_won_deal_value,
        approx_percentile(CASE WHEN deal_status = 'Signed / Ký hợp đồng' THEN vnd_amount END, 0.9) AS p90_won_deal_value,
        approx_percentile(
            CASE WHEN deal_status = 'Signed / Ký hợp đồng' AND created_at IS NOT NULL AND sign_date IS NOT NULL
                THEN datediff(sign_date, created_at) END, 0.5
        ) AS median_sales_cycle_days,
        approx_percentile(
            CASE WHEN deal_status = 'Signed / Ký hợp đồng' AND created_at IS NOT NULL AND sign_date IS NOT NULL
                THEN datediff(sign_date, created_at) END, 0.9
        ) AS p90_sales_cycle_days,
        MAX(CASE WHEN deal_status NOT IN ('Signed / Ký hợp đồng','Lost') THEN expected_deal_value ELSE 0 END)
            / NULLIF(SUM(CASE WHEN deal_status NOT IN ('Signed / Ký hợp đồng','Lost') THEN expected_deal_value ELSE 0 END), 0)
            AS largest_pipeline_deal_concentration
    FROM bi_silver.crm_deals
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
SELECT
    grain_type,
    period_start,
    period_month,
    period_quarter,
    period_year,
    customer_segment_l1,
    customer_segment_l2,
    customer_segment_l3,
    territory_name,
    channel,
    deal_stage_name,
    closed_deal_count,
    won_deal_count,
    lost_deal_count,
    won_deal_value,
    won_deal_count / NULLIF(closed_deal_count, 0) AS win_rate,
    lost_deal_count / NULLIF(closed_deal_count, 0) AS lost_rate,
    won_deal_value / NULLIF(won_deal_count, 0) AS average_won_deal_value,
    median_won_deal_value,
    p90_won_deal_value,
    total_sales_cycle_days,
    won_deals_with_valid_cycle,
    total_sales_cycle_days / NULLIF(won_deals_with_valid_cycle, 0) AS average_sales_cycle_days,
    median_sales_cycle_days,
    p90_sales_cycle_days,
    largest_pipeline_deal_concentration,
    -- Ép kiểu cụ thể thay vì để NULL tự do để tránh lỗi NullType trong Parquet
    CAST(NULL AS DOUBLE) AS stage_conversion_rate,
    CAST(NULL AS DOUBLE) AS average_stage_duration_days
FROM base
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