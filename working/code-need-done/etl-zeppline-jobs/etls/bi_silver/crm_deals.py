%livy.pyspark
tgt_table = "bi_silver.crm_deals"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_deals"

import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())


import json

SOC_PRODUCTS = set([
    "VCS-AJIANT",
    "VCS-NSM",
    "VCS-CYM",
])

def get_soc_group(allocated_products):
    if not allocated_products:
        return None

    try:
        products = json.loads(allocated_products)

        if not isinstance(products, list):
            return None

        categories = set(
            str(item.get("category")).strip().upper()
            for item in products
            if isinstance(item, dict) and item.get("category")
        )

        if any("MSS" in category for category in categories):
            return True

        if SOC_PRODUCTS.issubset(categories):
            return True

        return False

    except Exception:
        return False


get_soc_group_udf = F.udf(
    get_soc_group,
    StringType()
)

spark.udf.register(
    "get_soc_group_fn",
    get_soc_group,
    StringType()
)


spark.catalog.clearCache()

# 1) Config & refresh
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE crm_raw.deals")
spark.sql("REFRESH TABLE crm_raw.deleted_deals")
spark.sql("REFRESH TABLE crm_raw.deal_stages")
spark.sql("REFRESH TABLE crm_raw.cm_contracts")
spark.sql("REFRESH TABLE crm_raw.deal_payment_statuses")
spark.sql("REFRESH TABLE crm_raw.currencies")
spark.sql("REFRESH TABLE crm_raw.territories")
spark.sql("REFRESH TABLE crm_raw.deal_reasons")
spark.sql("REFRESH TABLE crm_raw.sales_accounts")

query = """
WITH deleted_ids AS (
  SELECT DISTINCT CAST(id AS STRING) AS id
  FROM crm_raw.deleted_deals
),
ranked_deals AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.id
      ORDER BY s.updated_at DESC, s.created_at DESC
    ) AS rank_deal
  FROM crm_raw.deals s
  LEFT ANTI JOIN deleted_ids d
    ON CAST(s.id AS STRING) = d.id
),
latest_deals AS (
  SELECT * FROM ranked_deals WHERE rank_deal = 1
),

last_sales_accounts AS (
  SELECT
    id,
    custom_field.cf_tax_code AS cf_tax_code, name
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC, created_at DESC
      ) AS rn
    FROM crm_raw.sales_accounts
  ) t
  WHERE rn = 1
),

last_cm_contracts AS (
  SELECT
    custom_field.cf__opp_id,
    custom_field.cf_sign_date,
    custom_field.cf_type
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
      ) AS rn
    FROM crm_raw.cm_contracts
  ) t
  WHERE rn = 1
),
last_currencies AS (
  SELECT id, currency_code, exchange_rate
  FROM crm_raw.currencies
  WHERE is_active = true
)

SELECT
  CAST(s.id AS STRING) AS deal_id,
  s.name AS deal_name,

  CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(s.amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS currency_amount,
  CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(s.base_currency_amount,'nan'),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS vnd_amount,

  CAST(TO_DATE(s.expected_close) as TIMESTAMP) AS expected_close_date,
  CAST(TO_DATE(s.closed_date) as TIMESTAMP) AS closed_date,
  TO_TIMESTAMP(s.stage_updated_time) AS stage_updated_time,

  CASE
    WHEN s.closed_date IS NOT NULL THEN DATEDIFF(TO_DATE(s.closed_date), TO_DATE(s.created_at))
    ELSE 0
  END AS days_to_close,
  DATEDIFF(CURRENT_DATE(), TO_DATE(s.updated_at)) AS days_since_last_update,

  s.custom_field.cf_lock_revenue AS is_deal_locked,
  COALESCE(s.custom_field.cf_usd_to_vnd, 0) AS usd_to_vnd,
  CAST(TO_DATE(s.custom_field.cf_get_live_date) as TIMESTAMP) AS get_live_date,
  COALESCE(s.custom_field.cf_periodicity, 'None') AS periodicity,
  s.custom_field.cf_select_viettel AS is_select_viettel,
  s.custom_field.cf_has_budget AS has_budget,

  sha2(lower(trim(CAST(s.custom_field.cf_alias AS STRING))), 256) AS company_id,
  s.custom_field.cf_alias AS company_alias,
  sa.cf_tax_code AS company_tax_code,

  CAST(ROUND(CAST(regexp_replace(COALESCE(NULLIF(NULLIF(s.custom_field.cf_budget,'nan'),''),'0'),',','') AS DOUBLE),2) AS DECIMAL(18,2)) AS budget_amount,
  COALESCE(trim(s.custom_field.cf_group), 'Standard') AS vvip_segment,

  CASE 
      WHEN s.custom_field.cf_alias in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Thị Trường Viettel'
      WHEN s.custom_field.cf_segment in ('International', 'Direct / Local Channel' , 'Viettel Global Partner' , 'Quốc tế ngoài', 'Thị Trường Viettel', 'Quốc tế', 'Quốc Tế Ngoài' ) AND s.custom_field.cf_alias not in ('Movitel', 'Natcom', 'Unitel', 'VTG', 'Bitel', 'Halotel', 'METFONE', 'Metfone B2B') THEN 'Quốc Tế Ngoài'
      WHEN s.custom_field.cf_segment = 'International' AND s.custom_field.cf_segment2 = 'Viettel Global Partner' THEN 'Thị Trường Viettel'
      WHEN s.custom_field.cf_segment = 'International' AND s.custom_field.cf_segment2 = 'Direct / Local Channel' THEN 'Quốc Tế Ngoài'
      WHEN s.custom_field.cf_segment = 'International' THEN 'Quốc Tế Ngoài'
      WHEN s.custom_field.cf_segment = 'Khách hàng ngoài' THEN 'Ngoài'
      ELSE s.custom_field.cf_segment
  END AS customer_segment_l1,
      
  -- COALESCE(s.custom_field.cf_segment,  'Chưa xác định') AS customer_segment_l1,
  COALESCE(s.custom_field.cf_segment2, '-') AS customer_segment_l2,
    
  COALESCE(s.custom_field.cf_segment3, 'Chưa xác định') AS customer_segment_l3,

  COALESCE(s.custom_field.cf_channel, 'N/A') AS channel,
  CAST(regexp_replace(trim(s.custom_field.cf_weight), '%', '') AS DOUBLE) AS opportunity_weight_pct,
  COALESCE(s.custom_field.cf_bidding_required, false) AS bidding_required,
  COALESCE(s.custom_field.cf_presales, 'N/A') AS presales_name,
  COALESCE(s.custom_field.cf_project_manager, 'N/A') AS project_manager,
  COALESCE(s.custom_field.cf_sale_admin, 'N/A') AS sales_admin,

  CAST(s.custom_field.cf_partner as STRING) AS partner_id,
  CAST(s.custom_field.cf_contract as STRING) AS contract_id,

  COALESCE(s.custom_field.cf_initial_source, 'N/A') AS initial_source,
  COALESCE(s.custom_field.cf_warm_up_source, 'N/A') AS warm_up_source,

  COALESCE(s.custom_field.cf__territory, 'Global Sale') AS territory_name,
  COALESCE(s.custom_field.cf__duration, -1) AS contract_duration_month,
  s.custom_field.cf__create_contract AS is_created_contract,

  CAST(TO_DATE(s.custom_field.cf__expire_date) as TIMESTAMP) AS expire_date,
  COALESCE(s.custom_field.cf__company, 'N/A')  AS company_name,
  COALESCE(s.custom_field.cf__address, 'N/A')  AS company_address,
  COALESCE(s.custom_field.cf__province, 'N/A') AS company_province,
  COALESCE(s.custom_field.cf__country, 'N/A')  AS company_country,
  COALESCE(s.custom_field.cf__email,'N/A')    AS company_email,
  COALESCE(s.custom_field.cf__phone, 'N/A')    AS company_phone,
  COALESCE(s.custom_field.cf__contact, 'N/A')  AS company_contact,
  --COALESCE(s.custom_field.cf_interested_products, '-')  AS interested_products,
  --COALESCE(s.custom_field.cf__currency, 'VND') AS currency_code,
  -- Phần thay đổi: Mapping currency_code từ currency_id
  COALESCE(lc.currency_code, 'N/A') AS currency_code,

  COALESCE(s.custom_field.cf__quotation_status, 'N/A') AS quotation_status,
  CAST(TO_DATE(s.custom_field.cf__fac_date) as TIMESTAMP) AS fac_date,
  COALESCE(s.custom_field.cf__am, 'N/A') AS am_username,
  s.custom_field.cf__check_change AS check_change,

  COALESCE(s.probability, 0) as probability,
  TO_TIMESTAMP(s.updated_at) AS updated_at,
  TO_TIMESTAMP(s.created_at) AS created_at,

  CAST(s.deal_stage_id AS STRING) AS deal_stage_id,
  UPPER(b.forecast_type) AS deal_status,

  CAST(s.deal_payment_status_id AS STRING) AS deal_payment_status_id,
  COALESCE(s.age, -1) as age,
  COALESCE(s.recent_note,'' ) as recent_note,

  TO_TIMESTAMP(s.upcoming_activities_time) AS next_scheduled_activity_time,
  TO_TIMESTAMP(s.last_assigned_at) AS last_assigned_at,

  UPPER(COALESCE(s.last_contacted_sales_activity_mode, 'UNKNOWN')) AS last_contacted_activity_status,
  TO_TIMESTAMP(s.last_contacted_via_sales_activity) AS last_contacted_activity_time,
  TO_TIMESTAMP(s.deal_prediction_last_updated_at) AS deal_prediction_last_updated_at,

  CAST(s.expected_deal_value AS DECIMAL(18,2)) AS expected_deal_value,
  CAST( COALESCE(s.rotten_days , -1) AS INT) AS signing_delay_days,

  CAST(s.owner_id AS STRING) AS am_user_id,
  uu.display_name as am_user_name,
  uu.team_name as am_team_name,
  
  CAST(s.sales_account_id AS STRING) AS sales_account_id,
  sa.name as sales_account_name,
  
  CAST(s.deal_type_id AS STRING) AS deal_type_id,
  CAST(s.deal_reason_id AS STRING) AS deal_reason_id,
  CAST(s.currency_id AS STRING) AS currency_id,

  CAST(lc.exchange_rate AS DOUBLE) AS vnd_to_usd,

  COALESCE(j.name, 'N/A') AS deal_payment_status,
  COALESCE(b.name, 'N/A') AS deal_stage_name,
  CAST(TO_DATE(i.cf_sign_date) as TIMESTAMP) AS sign_date,

  CASE
    WHEN lower(trim(b.name)) = 'lost' THEN 'LOST'
    WHEN lower(trim(b.name)) = 'new' THEN 'NEW'
    WHEN lower(trim(b.name)) = 'poc' THEN 'POC'
    WHEN lower(trim(b.name)) = 'quotation sent / báo giá' THEN 'QUOTATION_SENT'
    WHEN lower(trim(b.name)) = 'signed / ký hợp đồng' THEN 'SIGNED'
    WHEN lower(trim(b.name)) = 'negotiation / đàm phán' THEN 'NEGOTIATION'
    ELSE 'UNKNOWN'
  END AS deal_stage_code,

  CAST( COALESCE(
    TO_DATE(s.custom_field.cf__expire_date),
    TO_DATE(s.expected_close),
    TO_DATE(s.closed_date)
  ) as TIMESTAMP) AS due_date,


  CASE WHEN i.cf_sign_date IS NOT NULL THEN 1 ELSE 0 END AS is_signed,

  CASE
    WHEN s.probability = 100 THEN 'SUCCESS'
    WHEN s.probability > 10 AND s.probability < 80 THEN 'POTENTIAL'
    WHEN s.probability < 10 THEN 'FAILED'
    ELSE 'UNCERTAIN'
  END AS probability_bucket,
  i.cf_type as contract_type,

  COALESCE(dr.name, '') as deal_reason,
  COALESCE(internal_am_dictionary.internal_am_group , 'Chưa xác định') as am_group,
  s.custom_field.cf__deal_type as deal_type
  ,territories.name as am_territory_name
    
  , CASE
            WHEN s.custom_field.cf_segment = 'Nội bộ'
              OR (
                   s.custom_field.cf_segment = 'International'
               AND s.custom_field.cf_segment2  in ('Viettel Global Partner' , 'Thị trường') 
              )
              THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN s.custom_field.cf_segment = 'International'
             AND s.custom_field.cf_segment2  in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )
              THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN s.custom_field.cf_segment = 'Bộ Quốc phòng'
              THEN 'DT BQP (gồm cả SI BQP)'
            WHEN s.custom_field.cf_segment = 'Khách hàng ngoài'
              THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
          END
    AS report_customer_segment,

    CASE
              WHEN LOWER(COALESCE(s.custom_field.cf_channel, '')) LIKE '%partner%' THEN 'Partner'
              WHEN LOWER(COALESCE(s.custom_field.cf_channel, '')) LIKE '%direct%' THEN 'Direct'
              ELSE 'Khác'
            END
    AS channel_am,

    CASE
              WHEN COALESCE(s.custom_field.cf_segment3, '') IN ('GOV', 'BFSI', 'Năng lượng', 'DNL') THEN 'AM VVIP'
              ELSE 'Khác'
            END
    AS vvip_am,
    CASE
              WHEN COALESCE(s.custom_field.cf_segment, '') = 'Nội bộ' THEN 'AM Nội bộ'
              ELSE 'Khác'
            END
    AS internal_am,
    CASE WHEN s.custom_field.cf__quotations is not NULL THEN true ELSE false END as has_quotations,
    CASE WHEN s.custom_field.cf__global_discount is not NULL THEN true ELSE false END as has_global_discount,
    CASE WHEN s.custom_field.cf__allocated_products is not NULL THEN true ELSE false END  as has_allocated_products,
    CASE WHEN s.custom_field.cf__allocated_records is not NULL THEN true ELSE false END as has_allocated_records,
    get_soc_group_fn(s.custom_field.cf__allocated_products) AS is_soc_qualified

FROM latest_deals s
LEFT JOIN crm_raw.deal_stages b ON s.deal_stage_id = b.id
LEFT JOIN last_cm_contracts i ON CAST(s.id AS STRING) = CAST(i.cf__opp_id AS STRING)
LEFT JOIN crm_raw.deal_payment_statuses j ON s.deal_payment_status_id = j.id
LEFT JOIN crm_raw.deal_reasons dr ON s.deal_reason_id = dr.id
LEFT JOIN last_currencies lc ON s.currency_id = lc.id
LEFT JOIN last_sales_accounts sa ON CAST(s.sales_account_id AS STRING) = CAST(sa.id AS STRING)
LEFT JOIN crm_raw.internal_am_dictionary ON s.custom_field.cf_alias = internal_am_dictionary.company_alias
LEFT JOIN crm_raw.territories ON s.territory_id = territories.id
LEFT JOIN bi_silver.crm_users uu ON s.owner_id = uu.user_id

WHERE s.is_deleted = false
"""

df_deals = spark.sql(query)

# # 2) Drop đúng table + hard delete path chống double

df_deals = df_deals.withColumn(
    "customer_segment_l2",
    normalize_udf(F.col("customer_segment_l2"))
)
df_deals = df_deals.withColumn("customer_segment_l1", F.initcap(F.col("customer_segment_l1")))

# 3) Write
df_deals.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

# .save(tgt_path)
# .option("path", tgt_path) \
# .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("--------------------------------------------------")
print("THANH CONG: Bang crm_deals da duoc cap nhat.")
print("Tong so deals (unique id): " + str(df_deals.count()))
print("--------------------------------------------------")