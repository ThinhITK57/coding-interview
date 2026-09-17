# %livy.pyspark
tgt_table = "bi_silver.crm_revenue_reconciliation"
tgt_path  = "s3a://bi-silver/crm_revenue_reconciliation"


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
spark.sql("REFRESH TABLE crm_raw.revenue_reconciliation")
spark.sql("REFRESH TABLE crm_tool.actual_revenue")

query = """
SELECT
    -- Date
    COALESCE(
        CAST(invoice_date AS TIMESTAMP),
        TO_TIMESTAMP(invoice_date, '%Y-%m-%d'),
        TO_TIMESTAMP(invoice_date, '%Y-%m-%d %H:%M:%s'),
        TO_TIMESTAMP(invoice_date, '%d/%m/%Y')
    ) AS invoice_date,

    CAST( MONTH( COALESCE(
        CAST(invoice_date AS TIMESTAMP),
        TO_TIMESTAMP(invoice_date, '%Y-%m-%d'),
        TO_TIMESTAMP(invoice_date, '%Y-%m-%d %H:%M:%s'),
        TO_TIMESTAMP(invoice_date, '%d/%m/%Y')
    ) ) AS INTEGER ) AS report_month,

    billing_type,

    invoice_description AS transaction_description,

    -- Revenue
    CAST(COALESCE(vnd_amount, 0.00) AS DECIMAL(18, 2)) AS revenue_amount,

    CAST(0.00 AS DECIMAL(18, 2)) AS shared_revenue_amount,

    CAST(COALESCE(vnd_amount, 0.00) AS DECIMAL(18, 2)) AS actual_revenue_amount,

    CAST(COALESCE(vat_amount, 0.00) AS DECIMAL(18, 2)) AS vat_amount,

    CAST(COALESCE(gross_vnd_amount, 0.00) AS DECIMAL(18, 2)) AS gross_amount,

    CAST(0.00 AS DECIMAL(18, 2)) AS shared_revenue_from_mss_amount,

    revenue_type,

    -- Raw mới không có service_category , Mss/Chênh Lệch Tài Chính/Phần Mềm
    '-' AS service_category,

    -- Customer / Segment
    CASE
        WHEN t1.customer_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
        WHEN customer_segment_l1 = 'International' AND t1.customer_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN customer_segment_l1 in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế' ) AND t1.customer_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN customer_segment_l1 IS NULL OR TRIM(customer_segment_l1) = ''
            THEN 'Khác'
        ELSE customer_segment_l1
    END AS customer_segment_l1,
    
    -- Sử dụng lại dữ liệu excel
    CASE
        WHEN t1.customer_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
        WHEN viettel_segment = 'International' AND t1.customer_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN viettel_segment in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế' ) AND t1.customer_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc tế ngoài'
        WHEN viettel_segment IS NULL OR TRIM(viettel_segment) = '' THEN 'Khác'
        ELSE viettel_segment
    END AS revenue_segment_l1,

    '-' AS am_segment,

    CASE
        WHEN business_center IS NULL OR TRIM(business_center) = ''
            THEN 'Khác'
        ELSE business_center
    END AS business_center,

    CASE
        WHEN channel_name IS NULL OR TRIM(channel_name) = ''
            THEN 'Khác'
        ELSE channel_name
    END AS customer_channel,

    customer_alias AS customer_name,

    CASE
        WHEN department IS NULL OR TRIM(department) = ''
            THEN 'Khác'
        ELSE department
    END AS department_name,

    CASE
        WHEN customer_group IS NULL OR TRIM(customer_group) = ''
            THEN 'Khác'
        ELSE customer_group
    END AS vvip_segment,

    CASE
        WHEN am_username IS NULL OR TRIM(am_username) = ''
            THEN NULL
        ELSE am_username
    END AS account_manager_name,

    CASE
        WHEN presale_name IS NULL OR TRIM(presale_name) = ''
            THEN NULL
        ELSE presale_name
    END AS presale_name,

    revenue_type  AS revenue_classification,

    CASE
        WHEN soc_classification IS NULL OR TRIM(soc_classification) = ''
            THEN 'Khác'
        ELSE soc_classification
    END AS soc_classification,

    -- territory_name  AS market_segment,

    -- Additional dimensions
    CASE
        WHEN channel_name IS NULL OR TRIM(channel_name) = ''
            THEN 'Khác'
        ELSE channel_name
    END AS channel_name,

    CASE
        WHEN customer_alias IS NULL OR TRIM(customer_alias) = ''
            THEN 'Khác'
        ELSE customer_alias
    END AS customer_alias,

    CASE
        WHEN customer_id IS NULL OR TRIM(customer_id) = ''
            THEN '-'
        ELSE customer_id
    END AS customer_id,

    CASE
        WHEN customer_segment_l2 IS NULL OR TRIM(customer_segment_l2) = ''
            THEN 'Khác'
        ELSE customer_segment_l2
    END AS customer_segment_l2,

    CASE
        WHEN customer_segment_l3 IS NULL OR TRIM(customer_segment_l3) = ''
            THEN 'Khác'
        ELSE customer_segment_l3
    END AS customer_segment_l3,

    CASE
        WHEN deployment_type IS NULL OR TRIM(deployment_type) = ''
            THEN 'Khác'
        ELSE deployment_type
    END AS deployment_type,

    CASE
        WHEN parent_product_category IS NULL OR TRIM(parent_product_category) = ''
            THEN 'Khác'
        ELSE parent_product_category
    END AS parent_product_category,

    CASE
        WHEN partner_name IS NULL OR TRIM(partner_name) = ''
            THEN 'Khác'
        ELSE partner_name
    END AS partner_name,

    CASE
        WHEN t1.product_category IS NULL OR TRIM(t1.product_category) = ''
            THEN 'Khác'
        ELSE t1.product_category
    END AS product_category,

    t2.product_category_code,

    t2.product_service_group AS product_service_group,
    t2.product_service_group_vcs,

    CASE
        WHEN product_version IS NULL OR TRIM(product_version) = ''
            THEN 'Khác'
        ELSE product_version
    END AS product_version,

    CASE
        WHEN revenue_frequency_type IS NULL OR TRIM(revenue_frequency_type) = ''
            THEN 'Khác'
        ELSE revenue_frequency_type
    END AS revenue_frequency_type,

    CASE
        WHEN team_name IS NULL OR TRIM(team_name) = ''
            THEN 'Khác'
        ELSE team_name
    END AS team_name,

    CASE
        WHEN territory_name IS NULL OR TRIM(territory_name) = ''
            THEN 'Khác'
        ELSE territory_name
    END AS territory_name,

    CAST(
        COALESCE(
            NULLIF(TRIM(is_hold), ''),
            'false'
        ) AS BOOLEAN
    ) AS is_hold,
    contract_number

FROM crm_tool.actual_revenue t1
LEFT JOIN bi_silver.crm_product_service_group_mapping t2 on lower(t1.product_category) = lower(t2.product_category)
"""

df = spark.sql(query)


df = df.withColumn(
    "customer_segment_l2",
    normalize_udf(F.col("customer_segment_l2"))
)

df = df.withColumn(
    "product_category",
    normalize_udf(F.col("product_category"))
)

df = df.withColumn("revenue_segment_l1", F.initcap(F.col("revenue_segment_l1")))
df = df.withColumn("customer_segment_l1", F.initcap(F.col("customer_segment_l1")))
df = df.withColumn("customer_segment_l2", F.initcap(F.col("customer_segment_l2")))
df = df.withColumn("business_center", F.initcap(F.col("business_center")))

# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
   .save(tgt_path)

#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)
#   .save(tgt_path)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

