%livy.pyspark
tgt_table = "bi_silver.crm_expected_revenue"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_expected_revenue"


import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())

spark.catalog.clearCache()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.sql("REFRESH TABLE bi_silver.crm_users")

# =========================================================
# 2) Base: latest deals + filter deleted + signed only
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
        FROM bi_silver.crm_deals
    ) t
    WHERE rn = 1
)

SELECT
    ----------------------------------------------------------------------
    -- Payment
    ----------------------------------------------------------------------
    CAST(ar.recognition_date AS TIMESTAMP)                               AS payment_date,
    CAST(ar.recognition_date AS TIMESTAMP)                               AS payment_period_start_date,
    CAST(ar.recognition_date AS TIMESTAMP)                               AS payment_period_end_date,

    ----------------------------------------------------------------------
    -- Deal
    ----------------------------------------------------------------------
    ar.deal_id,
    d.deal_name,
    d.updated_at                                                         AS update_at,

    ----------------------------------------------------------------------
    -- Revenue
    ----------------------------------------------------------------------
    CAST(ar.product_total_value AS DECIMAL(18,2))                        AS product_total_value,
    CAST(ar.currency_amount AS DECIMAL(18,2))                            AS currency_amount,
    CAST(ar.allocated_revenue_amount AS DECIMAL(18,2)) AS vnd_amount,
    d.currency_code,

    CAST(ar.payment_index AS BIGINT)                                     AS payment_index,

    CAST(-1 AS BIGINT)                                                   AS used_days,

    CAST(d.usd_to_vnd AS DECIMAL(18,2))                                  AS exchange_rate,

    ----------------------------------------------------------------------
    -- Dates
    ----------------------------------------------------------------------
    CAST(ar.recognition_date AS TIMESTAMP)                               AS carry_over_date,
    CAST(ar.actual_start AS TIMESTAMP)                                   AS actual_start_date,
    CAST(ar.forecast_start AS TIMESTAMP)                                 AS forecast_start_date,

    YEAR(CAST(ar.recognition_date AS TIMESTAMP))
        = YEAR(current_date())                                             AS is_in_year,

    d.due_date,
    d.sign_date,
    d.expected_close_date,

    CAST(ar.contract_expire_date AS TIMESTAMP)                           AS contract_expire_date,

    d.fac_date,

    ----------------------------------------------------------------------
    -- Product
    ----------------------------------------------------------------------
    CAST(0 AS DECIMAL(10,2))                                          AS vat,

    ar.product_type,
    ar.product_version,
    ar.deployment_type,
    ar.product_category,
    t2.product_service_group_vcs,
    t2.product_service_group,
    t2.product_category_code,

    -1                                                AS product_category_index,

    ar.pid                                                               AS pricebook_id,
    ar.recurring                                                         AS is_recurring,

    d.territory_name,

    ar.pid                                                               AS product_index,

    ----------------------------------------------------------------------
    -- Customer
    ----------------------------------------------------------------------
    ar.customer_id,
    ar.customer_name,

    d.company_alias,

    COALESCE(ar.customer_group, d.vvip_segment)                        AS vvip_segment,

    ----------------------------------------------------------------------
    -- Deal info
    ----------------------------------------------------------------------
    d.deal_stage_name,
    d.deal_status,

    CAST(d.probability AS DOUBLE)                                        AS probability,
    CASE
        WHEN d.probability >= 70 THEN 'Cao'
        ELSE 'Thấp'
    END AS opportunity_level,

    ar.am_username,
    ar.team_name,

    ar.contract_id,
    ar.contract_number,

    d.contract_duration_month                                            AS contract_total_days,

    ar.revenue_type,
    
    CASE 
        WHEN d.company_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
        WHEN ar.customer_segment_l1 = 'International' AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN ar.customer_segment_l2 in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế' ) AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN ar.customer_segment_l1 = 'International' AND ar.customer_segment_l2 = 'Direct / Local Channel' THEN 'Quốc tế ngoài'
        WHEN ar.customer_segment_l1 = 'International' AND ar.customer_segment_l2 = 'Viettel Global Partner' THEN 'Thị Trường Viettel'
        WHEN ar.customer_segment_l1 = 'Khách hàng ngoài' THEN 'Ngoài'
        WHEN ar.customer_segment_l1 = 'Thị trường Viettel' THEN 'Thị Trường Viettel'
        ELSE ar.customer_segment_l1
    END AS revenue_segment_l1,

    CASE
        WHEN d.company_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
        WHEN d.customer_segment_l1 = 'International' AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN d.customer_segment_l2 in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế' ) AND d.company_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        
        WHEN d.customer_segment_l1 IN ('Khách hàng ngoài', 'Ngoài') THEN 'Ngoài'
        WHEN d.customer_segment_l1 = 'International' AND d.customer_segment_l2  = 'Direct / Local Channel'   THEN 'Quốc tế ngoài'
        WHEN d.customer_segment_l1 = 'International'  AND d.customer_segment_l2 = 'Viettel Global Partner'   THEN 'Thị trường Viettel'
        ELSE d.customer_segment_l1    END AS customer_segment_l1,
    COALESCE(ar.customer_segment_l2,d.customer_segment_l2)               AS customer_segment_l2,
    COALESCE(ar.customer_segment_l3,d.customer_segment_l3)               AS customer_segment_l3,

    d.sales_admin                                                        AS sales_admin_username,

    ar.allocated_record_id                                               AS contract_allocation_id,

    d.channel as channel_name,

    d.am_group,

    COALESCE(ar.deal_type,d.deal_type)                                   AS deal_type,

    d.am_territory_name,
    CASE
            WHEN ar.customer_segment_l1 = 'Nội bộ'
              OR (
                   ar.customer_segment_l1 = 'Thị trường Viettel'
               AND ar.customer_segment_l2  in ('Viettel Global Partner' , 'Thị trường') 
              )
              THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN ar.customer_segment_l1 = 'Quốc tế ngoài'
             AND ar.customer_segment_l2  in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )
              THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN ar.customer_segment_l1 = 'Bộ Quốc phòng'
              THEN 'DT BQP (gồm cả SI BQP)'
            WHEN ar.customer_segment_l1 = 'Khách hàng ngoài'
              THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
          END
    AS report_customer_segment,

    CASE
              WHEN LOWER(COALESCE(d.channel, '')) LIKE '%partner%' THEN 'Partner'
              WHEN LOWER(COALESCE(d.channel, '')) LIKE '%direct%' THEN 'Direct'
              ELSE 'Khác'
            END
    AS channel_am,

    CASE
              WHEN COALESCE(ar.customer_segment_l3, '') IN ('GOV', 'BFSI', 'Năng lượng', 'DNL') THEN 'AM VVIP'
              ELSE 'Khác'
            END
    AS vvip_am,
    CASE
              WHEN COALESCE(ar.customer_segment_l1, '') = 'Nội bộ' THEN 'AM Nội bộ'
              ELSE 'Khác'
            END
    AS internal_am,

    -1                                                AS forecast_category

FROM crm_tool.crm_allocated_revenue ar
LEFT JOIN deals_dedup d   ON ar.deal_id = d.deal_id
LEFT JOIN bi_silver.crm_product_service_group_mapping t2 on lower(t2.product_category) = lower(ar.product_category)
WHERE d.deal_stage_name not in ('Signed / Ký hợp đồng', 'Lost')
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
# 8) Write final table only
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
print("THANH CONG: crm_expected_revenue da duoc cap nhat")
print("Rows:", df.count())
print("--------------------------------------------------")