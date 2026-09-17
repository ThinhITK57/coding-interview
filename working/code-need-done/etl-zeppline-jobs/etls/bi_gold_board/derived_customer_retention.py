%livy.pyspark
# Retention:
# - Gia hạn
# 	+ Cùng ITEM  -> Renew
# 	+ Khác ITEM -> Upsales
# - mua mới
#   + Cùng ITEM
#   + Khác ITEM - cross SALES
# Định nghĩa giữ chân khách hàng:
# Contract Signed method = direct
# Đã mua 1 product_category (deal_type + contract_type = renew)
# fac_date không trống
# Thời gian truy vấn và báo cáo lấy theo fac_date
# Thời gian ký hợp đồng tiếp theo <6 tháng kể từ ngày hợp đồng gần nhất hết hạn
# Công thức tính CRR = (số khách hàng có renew theo sản phẩm dịch vụ)/(Tổng số khách hàng ) trong 1 chu kỳ tính (Quý, Năm)


spark.catalog.clearCache()

# 1) Config
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

tgt_table = "bi_gold.derived_customer_retention"
tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/derived_customer_retention"

# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

query = """
WITH customer_month_revenue AS (
    SELECT
        CAST(date_trunc('month', fac_date) AS date) AS month_start,
        CAST(date_trunc('quarter', fac_date) AS date) AS quarter_start,
        CAST(date_trunc('year', fac_date) AS date) AS year_start,
        company_id AS customer_id,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        product_category,
        territory_name,
        SUM(period_value) AS revenue_amount
    FROM bi_silver.crm_contract_allocations
    GROUP BY 1,2,3,4,5,6,7,8,9
),
customer_first_month AS (
    SELECT
        customer_id,
        MIN(month_start) AS first_revenue_month
    FROM customer_month_revenue
    GROUP BY 1
),
customer_base AS (
    SELECT
        r.month_start,
        r.quarter_start,
        r.year_start,
        r.customer_id,
        r.customer_segment_l1,
        r.customer_segment_l2,
        r.customer_segment_l3,
        r.product_category,
        r.territory_name,
        r.revenue_amount,
        CASE WHEN f.first_revenue_month = r.month_start THEN 1 ELSE 0 END AS is_new_customer
    FROM customer_month_revenue r
    LEFT JOIN customer_first_month f
        ON r.customer_id = f.customer_id
),
customer_roll AS (
    SELECT
        'MONTH' AS grain_type,
        month_start AS period_start,
        month_start AS period_month,
        quarter_start AS period_quarter,
        year_start AS period_year,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        product_category,
        territory_name,
        COUNT(DISTINCT customer_id) AS opening_customer_count,
        COUNT(DISTINCT CASE WHEN is_new_customer = 1 THEN customer_id END) AS new_customer_count,
        CAST(0 AS BIGINT) AS churned_customer_count,
        SUM(revenue_amount) AS beginning_period_revenue,
        SUM(revenue_amount) AS retained_revenue,
        CAST(0 AS DECIMAL(18,2)) AS expansion_revenue,
        CAST(0 AS DECIMAL(18,2)) AS churned_revenue,
        SUM(revenue_amount) / NULLIF(COUNT(DISTINCT customer_id),0) AS average_revenue_per_active_customer
    FROM customer_base
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),
customer_top AS (
    SELECT
        month_start,
        customer_segment_l1,
        customer_segment_l2,
        customer_segment_l3,
        product_category,
        territory_name,
        MAX(customer_revenue) / NULLIF(SUM(customer_revenue),0) AS top_customer_revenue_concentration
    FROM (
        SELECT
            month_start,
            customer_segment_l1,
            customer_segment_l2,
            customer_segment_l3,
            product_category,
            territory_name,
            customer_id,
            SUM(revenue_amount) AS customer_revenue
        FROM customer_base
        GROUP BY 1,2,3,4,5,6,7
    ) x
    GROUP BY 1,2,3,4,5,6
)
SELECT
    r.grain_type,
    r.period_start,
    r.period_month,
    r.period_quarter,
    r.period_year,
    r.customer_segment_l1,
    r.customer_segment_l2,
    r.customer_segment_l3,
    r.product_category,
    r.territory_name,
    r.opening_customer_count,
    r.new_customer_count,
    r.churned_customer_count,
    r.churned_customer_count / NULLIF(r.opening_customer_count,0) AS customer_churn_rate,
    r.beginning_period_revenue,
    r.retained_revenue,
    r.expansion_revenue,
    r.churned_revenue,
    (r.retained_revenue - r.churned_revenue) / NULLIF(r.beginning_period_revenue,0) AS gross_revenue_retention_rate,
    (r.retained_revenue + r.expansion_revenue - r.churned_revenue) / NULLIF(r.beginning_period_revenue,0) AS net_revenue_retention_rate,
    r.expansion_revenue / NULLIF(r.beginning_period_revenue,0) AS expansion_revenue_rate,
    r.churned_revenue / NULLIF(r.beginning_period_revenue,0) AS revenue_churn_rate,
    r.average_revenue_per_active_customer,
    t.top_customer_revenue_concentration
FROM customer_roll r
LEFT JOIN customer_top t
    ON r.period_month = t.month_start
   AND COALESCE(r.customer_segment_l1,'') = COALESCE(t.customer_segment_l1,'')
   AND COALESCE(r.customer_segment_l2,'') = COALESCE(t.customer_segment_l2,'')
   AND COALESCE(r.customer_segment_l3,'') = COALESCE(t.customer_segment_l3,'')
   AND COALESCE(r.product_category,'') = COALESCE(t.product_category,'')
   AND COALESCE(r.territory_name,'') = COALESCE(t.territory_name,'')
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