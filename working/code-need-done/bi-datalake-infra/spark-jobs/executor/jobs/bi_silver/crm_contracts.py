# %livy.pyspark
tgt_table = "bi_silver.crm_contracts"
tgt_path  = "s3a://bi-silver/crm_contracts"


import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())

# Làm mới danh mục file nguồn
spark.sql("REFRESH TABLE crm_raw.cm_contracts")
spark.catalog.clearCache()
sql_query = """
WITH last_cm_contracts AS (
  SELECT *
  FROM (
    SELECT
      ss.*,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
      ) rn
    FROM crm_raw.cm_contracts ss
  ) t
  WHERE rn = 1
)
SELECT
  CAST(c.id AS STRING) AS id,
  c.name,
  CAST(c.owner_id AS STRING) as user_am_id,
  uu.display_name as user_am_name,
  
  CAST(c.updater_id AS STRING) as updater_id,
  uuu.display_name as updater_name,
  
  CAST(c.custom_field.cf_opportunity AS STRING) AS deal_id,

  c.custom_field.cf_contract_id AS contract_number,
  c.custom_field.cf_type        AS contract_type,
  LOWER(TRIM(c.custom_field.cf_signing_method))        AS signing_method,

  CASE
    WHEN c.custom_field.cf_signing_method IS NULL THEN 'UNKNOWN'
    WHEN UPPER(TRIM(c.custom_field.cf_signing_method)) IN ('INDIRECT','GIÁN TIẾP','INDIRECT / GIÁN TIẾP') THEN 'INDIRECT'
    WHEN UPPER(TRIM(c.custom_field.cf_signing_method)) IN ('DIRECT','TRỰC TIẾP','DIRECT / TRỰC TIẾP') THEN 'DIRECT'
    ELSE 'UNKNOWN'
  END AS signing_method_code,

  c.custom_field.cf_company AS sale_account_id,
  c.custom_field.cf_alias   AS company_alias,
  c.custom_field.cf_alias   AS customer_name,
  c.custom_field.cf_tax_code AS tax_code,

  CASE WHEN c.custom_field.cf_group   IS NULL THEN 'Chưa xác định' ELSE c.custom_field.cf_group   END AS customer_group_vi,
  
  CASE 
      WHEN c.custom_field.cf_segment = 'International' AND c.custom_field.cf_segment2 = 'Direct / Local Channel' THEN 'Quốc tế ngoài'
      WHEN c.custom_field.cf_segment = 'International' AND c.custom_field.cf_segment2 = 'Viettel Global Partner' THEN 'Thị Trường Viettel'
      WHEN c.custom_field.cf_segment = 'International' THEN 'Quốc tế ngoài'
      WHEN c.custom_field.cf_segment = 'Khách hàng ngoài' THEN 'Ngoài'
      ELSE c.custom_field.cf_segment
  END AS customer_segment_l1,
  
  -- CASE WHEN c.custom_field.cf_segment IS NULL THEN 'UNKNOWN'       ELSE c.custom_field.cf_segment END AS customer_segment_l1,
  COALESCE(c.custom_field.cf_segment2, '-') AS customer_segment_l2,
  
  CASE WHEN c.custom_field.cf_segment3 IS NULL THEN 'UNKNOWN'      ELSE c.custom_field.cf_segment3 END AS customer_segment_l3,

  lower(TRIM(c.custom_field.cf_status)) as status,
  
  CASE
    WHEN c.custom_field.cf_status IS NULL THEN 'UNKNOWN'
    WHEN UPPER(TRIM(c.custom_field.cf_status)) IN ('DEPLOYMENT','DEPLOYMENT / TRIỂN KHAI') THEN 'DEPLOYMENT'
    WHEN UPPER(TRIM(c.custom_field.cf_status)) IN ('SIGNED','SIGNED / KÝ HỢP ĐỒNG') THEN 'SIGNED'
    WHEN UPPER(TRIM(c.custom_field.cf_status)) IN ('DEPLOYMENT COMPLETED','DEPLOYMENT COMPLETED / HOÀN THÀNH TRIỂN KHAI') THEN 'DEPLOYMENT_COMPLETED'
    ELSE 'UNKNOWN'
  END AS status_code,

  CASE
    WHEN c.custom_field.cf_status IS NULL THEN 'KHÔNG XÁC ĐỊNH'
    WHEN UPPER(TRIM(c.custom_field.cf_status)) IN ('DEPLOYMENT','DEPLOYMENT / TRIỂN KHAI') THEN 'TRIỂN KHAI'
    WHEN UPPER(TRIM(c.custom_field.cf_status)) IN ('SIGNED','SIGNED / KÝ HỢP ĐỒNG') THEN 'KÝ HỢP ĐỒNG'
    WHEN UPPER(TRIM(c.custom_field.cf_status)) IN ('DEPLOYMENT COMPLETED','DEPLOYMENT COMPLETED / HOÀN THÀNH TRIỂN KHAI') THEN 'HOÀN THÀNH TRIỂN KHAI'
    ELSE 'KHÔNG XÁC ĐỊNH'
  END AS status_vi,

  CASE WHEN c.custom_field.cf_group  IS NULL or c.custom_field.cf_group = '' THEN 'Standard' ELSE trim(c.custom_field.cf_group)   END AS vvip_segment,
  CASE WHEN trim(c.custom_field.cf_group) = 'Khách hàng VIP, VVIP' THEN 1 ELSE 0 END AS is_vip_customer,
  CASE WHEN trim(c.custom_field.cf_group) = 'Khách hàng lớn' THEN 1 ELSE 0 END AS is_enterprise_customer,

  CASE
    WHEN c.custom_field.cf_segment3 IN (
      'GOV','Province','Energy',
      'doanh nghiệp nhà nước','tập đoàn nhà nước','cơ quan nhà nước','Doanh nghiệp 100% vốn Nhà nước'
    ) THEN 1 ELSE 0
  END AS is_state_owned,

  CASE
    WHEN c.custom_field.cf_segment = 'Khách hàng ngoài'
      AND (c.custom_field.cf_segment3 IS NULL OR c.custom_field.cf_segment3 NOT IN ('GOV','Province','Energy'))
    THEN 1 ELSE 0
  END AS is_private_enterprise,

  CASE WHEN upper(c.custom_field.cf_segment3) = 'BFSI' THEN 1 ELSE 0 END AS is_banking_group,
  CASE WHEN c.custom_field.cf_segment = 'International' THEN 1 ELSE 0 END AS is_international_client,
  CASE WHEN c.custom_field.cf_segment = 'Nội bộ' THEN 1 ELSE 0 END AS is_internal_client,

  CAST(c.custom_field.cf_sign_date as TIMESTAMP)   AS sign_date,
  CAST(c.custom_field.cf_fac_date as TIMESTAMP)    AS fac_date,
  COALESCE(c.custom_field.cf_duration, -1)    AS duration,
  CAST(c.custom_field.cf_expire_date as TIMESTAMP) AS expire_date,

  COALESCE(c.custom_field.cf_usd_to_vnd, 0)   AS usd_exchange_rate,
  --COALESCE(c.custom_field.cf_deployed_products, '-')   AS deployed_product_categories,
  c.custom_field.cf_currency    AS currency_code,
  -- c.custom_field.cf_revenue     AS revenue,
  CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(c.custom_field.cf_revenue,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS contract_amount,
  COALESCE(c.custom_field.cf_vat, 0)         AS vat,
  -- c.custom_field.cf_vcs_revenue AS vcs_revenue,
  CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(c.custom_field.cf_vcs_revenue,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS vcs_contract_amount,
  -- c.custom_field.cf_viettel_revenue AS viettel_revenue,
  CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(c.custom_field.cf_viettel_revenue,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS viettel_contract_amount,
  c.custom_field.cf_partner     AS partner_id
  , CAST(created_at as TIMESTAMP) AS created_at
  , CAST(updated_at as TIMESTAMP) AS updated_at

FROM last_cm_contracts c
LEFT JOIN bi_silver.crm_users uu ON c.owner_id = uu.user_id
LEFT JOIN bi_silver.crm_users uuu ON c.updater_id = uuu.user_id
"""

df = spark.sql(sql_query)
# Xác định ngày ký hợp đồng đầu tiên theo khách hàng
customer_first_contract_df = (
    df
    .filter(
        F.col("tax_code").isNotNull()
        & (F.trim(F.col("tax_code")) != "")
        & F.col("sign_date").isNotNull()
    )
    .groupBy("tax_code")
    .agg(
        F.min("sign_date").alias("first_contract_sign_date")
    )
)

df = (
    df
    .join(
        customer_first_contract_df,
        on="tax_code",
        how="left"
    )
    .withColumn(
        "is_first_contract",
        F.when(
            F.to_date(F.col("sign_date"))
            == F.to_date(F.col("first_contract_sign_date")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

df = df.withColumn(
    "customer_segment_l2",
    normalize_udf(F.col("customer_segment_l2"))
)

# 4) Save
df.repartition(1).write  \
  .mode("overwrite") \
  .format("parquet") \
 .save(tgt_path)

# .save(tgt_path)
# .option("path", tgt_path) \
# .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
# 5) Quick check
spark.sql("SELECT count(*) AS cnt FROM " + tgt_table).show()
