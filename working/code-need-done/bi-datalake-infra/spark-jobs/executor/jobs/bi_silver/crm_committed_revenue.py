# %livy.pyspark
tgt_table = "bi_silver.crm_committed_revenue"
tgt_path  = "s3a://bi-silver/crm_committed_revenue"


import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.sql("REFRESH TABLE bi_silver.crm_users")

# =========================================================
# 2) Base giống crm_payment_forecast
spark.catalog.clearCache()
# =========================================================

df = spark.sql("""
WITH deals_dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY deal_id
                   ORDER BY updated_at DESC
               ) AS rn
        FROM  bi_silver.crm_deals
    ) t
    WHERE rn = 1
),
contracts_dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY sign_date DESC
               ) AS rn
        FROM  bi_silver.crm_contracts
    ) t
    WHERE rn = 1
)

SELECT
    -- Report
    CAST(ar.recognition_date AS TIMESTAMP)                                        AS report_date,
    YEAR(CAST(ar.recognition_date AS TIMESTAMP))                                  AS report_year,
    MONTH(CAST(ar.recognition_date AS TIMESTAMP))                                 AS report_month,

    -- Product
    -- ar.product_category_code,
    ar.product_category,

    CASE
        WHEN c.contract_type = 'New' THEN 'DT mới'
        WHEN c.contract_type = 'Renew' THEN 'DT renew'
        WHEN c.contract_type = 'Upsales' THEN 'DT Upsales'
        ELSE 'N/A'
    END AS product_growth_type,

    t2.product_service_group_vcs,
    t2.product_service_group,
    t2.product_category_code,
    
    CAST(ar.allocated_revenue_amount AS DECIMAL(18,2)) AS revenue_amount,
    d.usd_to_vnd,
    COALESCE(d.company_alias, c.company_alias) AS company_alias,
    ar.revenue_type,
    d.channel as channel_name,

    CASE
        WHEN ar.product_category_code IN (
            'VCS001','VCS002','VCS003','VCS004','VCS005','VCS006',
            'VCS007','VCS008','VCS009','VCS010','VCS011','VCS012'
        ) THEN true
        ELSE false
    END AS is_soc,

    'N/A' AS department_alias,
    
    CASE 
        WHEN d.company_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
        WHEN ar.customer_segment_l1 = 'International' AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc Tế Ngoài'
        WHEN ar.customer_segment_l2 in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế' ) AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN ar.customer_segment_l1 = 'International' AND ar.customer_segment_l2 = 'Direct / Local Channel' THEN 'Quốc tế ngoài'
        WHEN ar.customer_segment_l1 = 'International' AND ar.customer_segment_l2 = 'Viettel Global Partner' THEN 'Thị Trường Viettel'
        WHEN ar.customer_segment_l1 = 'Khách hàng ngoài' THEN 'Ngoài'
        ELSE ar.customer_segment_l1
    END AS revenue_segment_l1,

    CASE
        WHEN d.company_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
        WHEN d.customer_segment_l1 = 'International' AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN d.customer_segment_l2 in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế' ) AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        
        WHEN COALESCE( d.customer_segment_l1, c.customer_segment_l1) IN ('Khách hàng ngoài', 'Ngoài') THEN 'Ngoài'
        WHEN COALESCE(  d.customer_segment_l1, c.customer_segment_l1) = 'International' AND COALESCE( ar.customer_segment_l2, d.customer_segment_l2,c.customer_segment_l2) = 'Direct / Local Channel'         THEN 'Quốc tế ngoài'
        WHEN COALESCE( d.customer_segment_l1, c.customer_segment_l1) = 'International'  AND COALESCE(ar.customer_segment_l2,d.customer_segment_l2,c.customer_segment_l2) = 'Viettel Global Partner'
        THEN 'Thị trường Viettel'
        ELSE COALESCE( d.customer_segment_l1, c.customer_segment_l1)    END AS customer_segment_l1,

    COALESCE(ar.customer_segment_l2, d.customer_segment_l2, c.customer_segment_l2) AS customer_segment_l2,
    COALESCE(ar.customer_segment_l3, d.customer_segment_l3, c.customer_segment_l3) AS customer_segment_l3,

    'N/A' AS revenue_group,

    CAST(c.vat AS INTEGER) AS vat,
    d.deal_payment_status AS payment_stage,
    ar.am_username,
    d.presales_name,
    COALESCE(d.currency_code, c.currency_code) AS currency_code,
    'N/A' AS business_unit_level_1,
    COALESCE(ar.territory_name, d.territory_name) AS territory_name,

    ar.deal_id,
    d.deal_name,
    d.updated_at AS update_at,
    d.currency_amount,

    CASE
        WHEN ar.actual_start IS NOT NULL
          OR ar.fac_date IS NOT NULL
        THEN true
        ELSE false
    END AS is_actual,

    d.due_date,
    ar.deployment_type,
    ar.pid AS pricebook_id,
    ar.recurring AS is_recurring,
    ar.pid AS product_index,
    ar.customer_id,
    ar.customer_name,
    COALESCE(ar.customer_group, d.vvip_segment, c.vvip_segment) AS vvip_segment,

    -1 AS product_category_index,
    ar.parent_product_category,

    ar.contract_id,
    CAST(ar.payment_index AS BIGINT) AS payment_index,
    ar.allocated_record_id AS contract_allocation_id,

    CAST(ar.recognition_date AS TIMESTAMP) AS payment_date,
    YEAR(CAST(ar.recognition_date AS TIMESTAMP)) = YEAR(current_date()) AS is_in_year,

    CAST(ar.recognition_date AS TIMESTAMP) AS payment_period_start_date,
    CAST(ar.recognition_date AS TIMESTAMP) AS payment_period_end_date,

    CAST(ar.product_total_value AS DECIMAL(18,2)) AS product_total_value,
    CAST(ar.allocated_revenue_amount AS DECIMAL(18,2)) AS vnd_amount,
    
    -1 AS used_days,

    CAST(c.usd_exchange_rate AS DECIMAL(18,2)) AS exchange_rate,

    COALESCE(d.sign_date, c.sign_date) AS sign_date,
    COALESCE(d.fac_date, c.fac_date) AS fac_date,

    c.expire_date AS contract_expire_date,
    ar.product_type,
    ar.product_version,
    d.deal_stage_name,
    d.deal_status,
    ar.team_name,

    CAST(d.probability AS DOUBLE) AS probability,

    'Đã ký' AS opportunity_level,

    COALESCE(
        ar.contract_number,
        c.contract_number
    ) AS contract_number,

    CAST(c.duration AS BIGINT) AS contract_total_days,

    d.sales_admin AS sales_admin_username,

    d.am_group,

    COALESCE(
        ar.deal_type,
        d.deal_type
    ) AS deal_type,

    d.am_territory_name,

    CASE
        WHEN COALESCE(
                ar.customer_segment_l1,
                d.customer_segment_l1,
                c.customer_segment_l1
            ) = 'Nội bộ'
        OR (
            COALESCE(
                ar.customer_segment_l1,
                d.customer_segment_l1,
                c.customer_segment_l1
            ) =  'Thị Trường Viettel'
            AND COALESCE(
                ar.customer_segment_l2,
                d.customer_segment_l2,
                c.customer_segment_l2
            ) IN ('Viettel Global Partner', 'Thị trường')
        )
        THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'

        WHEN COALESCE(
                ar.customer_segment_l1,
                d.customer_segment_l1,
                c.customer_segment_l1
            ) = 'Quốc tế ngoài'
        AND COALESCE(
                ar.customer_segment_l2,
                d.customer_segment_l2,
                c.customer_segment_l2
            ) IN ('Quốc tế ngoài', 'Direct / Local Channel')
        THEN 'DT Quốc tế (KH ngoài QT)'

        WHEN COALESCE(
                ar.customer_segment_l1,
                d.customer_segment_l1,
                c.customer_segment_l1
            ) = 'Bộ Quốc phòng'
        THEN 'DT BQP (gồm cả SI BQP)'

        WHEN COALESCE(
                ar.customer_segment_l1,
                d.customer_segment_l1,
                c.customer_segment_l1
            ) = 'Khách hàng ngoài'
        THEN 'DT ngoài trong nước (gồm cả SI ngoài)'

        ELSE 'Khác'
    END AS report_customer_segment,

    CASE
        WHEN LOWER(COALESCE(d.channel, '')) LIKE '%partner%'
            THEN 'Partner'
        WHEN LOWER(COALESCE(d.channel, '')) LIKE '%direct%'
            THEN 'Direct'
        ELSE 'Khác'
    END AS channel_am,

    CASE
        WHEN COALESCE(
                ar.customer_segment_l3,
                d.customer_segment_l3,
                c.customer_segment_l3
            ) IN ('GOV', 'BFSI', 'Năng lượng', 'DNL')
            THEN 'AM VVIP'
        ELSE 'Khác'
    END AS vvip_am,

    CASE
        WHEN COALESCE(
                ar.customer_segment_l1,
                d.customer_segment_l1,
                c.customer_segment_l1
            ) = 'Nội bộ'
            THEN 'AM Nội bộ'
        ELSE 'Khác'
    END AS internal_am,

    -1 AS forecast_category,

    CAST(ar.recognition_date AS TIMESTAMP) AS carry_over_date,

    CAST(ar.actual_start AS TIMESTAMP) AS actual_start_date,

    CAST(ar.forecast_start AS TIMESTAMP) AS forecast_start_date

FROM crm_tool.crm_allocated_revenue ar
LEFT JOIN deals_dedup d
    ON ar.deal_id = d.deal_id
LEFT JOIN contracts_dedup c    ON ar.contract_id = c.id
LEFT JOIN bi_silver.crm_product_service_group_mapping t2 on lower(t2.product_category) = lower(ar.product_category)

WHERE d.deal_stage_name = 'Signed / Ký hợp đồng'
""")

df = df.withColumn(
    "product_category",
    normalize_udf(F.col("product_category"))
)

df = df.withColumn(
    "customer_segment_l2",
    normalize_udf(F.col("customer_segment_l2"))
)

df = df.withColumn("revenue_segment_l1", F.initcap(F.col("revenue_segment_l1")))
df = df.withColumn("customer_segment_l1", F.initcap(F.col("customer_segment_l1")))
df = df.withColumn("customer_segment_l2", F.initcap(F.col("customer_segment_l2")))

# =========================================================
# 8) Write final table
# =========================================================
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(tgt_path)

# .save(tgt_path)
# .option("path", tgt_path) \
# .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("--------------------------------------------------")
print("THANH CONG: crm_committed_revenue da duoc cap nhat")
print("Rows:", df.count())
print("--------------------------------------------------")