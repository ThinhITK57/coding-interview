# %livy.pyspark

# =========================================================
# 0. TARGET
# =========================================================

tgt_table = "crm_silver.crm_mart_deals"
tgt_path  = "s3a://vcs-silver/crm-silver/crm_mart_deals"

spark.catalog.clearCache()

# =========================================================
# 1. REFRESH SOURCES
# =========================================================
spark.sql("REFRESH TABLE crm_raw.deleted_deals")
spark.sql("REFRESH TABLE bi_silver.crm_users")
spark.sql("REFRESH TABLE crm_raw.deals")
spark.sql("REFRESH TABLE crm_raw.deal_stages")
spark.sql("REFRESH TABLE crm_raw.currencies")
spark.sql("REFRESH TABLE bi_silver.crm_contracts")
spark.sql("REFRESH TABLE bi_silver.crm_partners")
spark.sql("REFRESH TABLE bi_silver.crm_sales_accounts")
spark.sql("REFRESH TABLE crm_raw.internal_am_dictionary")
spark.sql("REFRESH TABLE crm_raw.territories")
# =========================================================
# 2. QUERY
# =========================================================
query = """
WITH deleted_ids AS (
  SELECT DISTINCT CAST(id AS BIGINT) AS id
  FROM crm_raw.deleted_deals
),
user_team AS (
  SELECT CAST(user_id AS STRING) AS am_user_id,
         MAX(NULLIF(TRIM(CAST(email AS STRING)), '')) AS am_email,
         CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(CASE WHEN team_name IS NOT NULL AND TRIM(CAST(team_name AS STRING)) <> '' THEN TRIM(CAST(team_name AS STRING)) END))) AS team_name
  FROM bi_silver.crm_users
  GROUP BY CAST(user_id AS STRING)
),
ranked_deals AS (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY s.updated_at_ts DESC, s.id DESC) AS rn
  FROM crm_raw.deals s
  LEFT JOIN deleted_ids d ON s.id = d.id
  WHERE d.id IS NULL AND s.is_deleted = false
),
latest_deals AS (
  SELECT *
  FROM ranked_deals
  WHERE rn = 1
),
last_deal_stages AS (
  SELECT *
  FROM (
    SELECT ss.*, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at_ts DESC, id DESC) AS rn
    FROM crm_raw.deal_stages ss
  ) t
  WHERE rn = 1
),
last_currencies AS (
  SELECT id, currency_code, exchange_rate
  FROM (
    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.crawled_at_ts DESC, c.id DESC) AS rn
    FROM crm_raw.currencies c
    WHERE c.is_active = true
  ) t
  WHERE rn = 1
),
last_cm_contracts AS (
  SELECT CAST(id AS STRING) AS contract_id,
         CAST(deal_id AS STRING) AS deal_id,
         CAST(name AS STRING) AS contract_name,
         CAST(contract_type AS STRING) AS contract_type,
         CAST(contract_number AS STRING) AS contract_number,
         CAST(sign_date AS TIMESTAMP) AS sign_date,
         CAST(fac_date AS TIMESTAMP) AS fac_date,
         CAST(expire_date AS TIMESTAMP) AS expire_date,
         CAST(partner_id AS STRING) AS contract_partner_id
  FROM bi_silver.crm_contracts
),
last_cm_partners AS (
  SELECT CAST(id AS STRING) AS partner_id,
         CAST(partner_name AS STRING) AS partner_name
  FROM bi_silver.crm_partners
),
last_sales_accounts AS (
  SELECT CAST(id AS STRING) AS customer_id,
         CAST(name AS STRING) AS customer_name,
         CAST(company_alias AS STRING) AS customer_alias,
         CAST(tax_code AS STRING) AS customer_tax_code
  FROM bi_silver.crm_sales_accounts
)
SELECT
  CAST(s.id AS STRING) AS deal_id,
  CAST(s.name AS STRING) AS deal_name,
  CAST(COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf_contract AS STRING)), ''), i.contract_id) AS STRING) AS contract_id,
  NULLIF(TRIM(CAST(i.contract_name AS STRING)), '') AS contract_name,
  s.probability,
  CAST(LOWER(TRIM(b.name)) AS STRING) AS deal_stage_name,
  CAST(s.custom_field.cf_sale_admin AS STRING) AS sale_admin,
  CAST(s.sales_account_id AS STRING) AS customer_id,
  NULLIF(TRIM(CAST(sa.customer_name AS STRING)), '') AS customer_name,
  NULLIF(TRIM(CAST(sa.customer_alias AS STRING)), '') AS customer_alias,
  NULLIF(TRIM(CAST(sa.customer_tax_code AS STRING)), '') AS customer_tax_code,
  CAST(COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf_partner AS STRING)), ''), i.contract_partner_id) AS STRING) AS partner_id,
  NULLIF(TRIM(CAST(cp.partner_name AS STRING)), '') AS partner_name,
  CAST(COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf__territory AS STRING)), ''), 'Global Sale') AS STRING) AS territory_name,
  NULLIF(TRIM(CAST(s.custom_field.cf__am AS STRING)), '') AS am_username,
  CAST(s.owner_id AS STRING) AS am_user_id,
  NULLIF(TRIM(CAST(cu.am_email AS STRING)), '') AS am_email,
  CAST(COALESCE(cu.team_name, '-') AS STRING) AS team_name,
  CAST(COALESCE(CAST(s.custom_field.cf__fac_date AS TIMESTAMP), i.fac_date) AS TIMESTAMP) AS fac_date,
  CAST(COALESCE(i.sign_date, CAST(s.closed_date AS TIMESTAMP)) AS TIMESTAMP) AS sign_date,
  CAST(COALESCE(CAST(s.custom_field.cf__expire_date AS TIMESTAMP), i.expire_date) AS TIMESTAMP) AS contract_expire_date,
  CAST(s.custom_field.cf__products AS STRING) AS raw_cf_products,
  CAST(s.custom_field.cf__allocated_products AS STRING) AS raw_cf_allocated_products,
  CAST(s.custom_field.cf__allocated_records AS STRING) AS raw_cf_allocated_records,
  CAST(COALESCE(lc.currency_code, 'N/A') AS STRING) AS currency_code,
  CAST(COALESCE(CAST(s.custom_field.cf__expire_date AS TIMESTAMP), i.expire_date, CAST(s.closed_date AS TIMESTAMP)) AS TIMESTAMP) AS due_date,
  CAST(s.created_at AS TIMESTAMP) AS created_at,
  CAST(s.expected_close AS TIMESTAMP) AS expected_close,
  CAST(s.closed_date AS TIMESTAMP) AS closed_date,
  CAST(s.updated_at AS TIMESTAMP) AS updated_at,
  CAST(CASE WHEN s.custom_field.cf_group IS NULL OR TRIM(CAST(s.custom_field.cf_group AS STRING)) = '' THEN 'Unknown' ELSE TRIM(CAST(s.custom_field.cf_group AS STRING)) END AS STRING) AS customer_group,
  CAST(COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf_segment AS STRING)), ''), 'UNKNOWN') AS STRING) AS customer_segment_l1,
  CAST(COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf_segment2 AS STRING)), ''), '-') AS STRING) AS customer_segment_l2,
  CAST(COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf_segment3 AS STRING)), ''), '-') AS STRING) AS customer_segment_l3,
  COALESCE(CAST(s.custom_field.cf_usd_to_vnd AS DOUBLE), CAST(0 AS DOUBLE)) AS usd_to_vnd,
  CAST(i.contract_type AS STRING) AS contract_type,
  CAST(i.contract_number AS STRING) AS contract_number,
  CAST(s.custom_field.cf_channel AS STRING) AS channel_name,
  CAST(COALESCE(iad.internal_am_group, CASE WHEN s.custom_field.cf_segment3 IN ('GOV', 'BFSI', 'Năng lượng', 'DNL') THEN s.custom_field.cf_segment3 END) AS STRING) AS am_group,
  CAST(s.custom_field.cf__deal_type AS STRING) AS deal_type,
  CAST(territories.name AS STRING) AS am_territory_name,
  CAST(s.forecast_category AS STRING) AS forecast_category,
  CAST(
    CASE
      WHEN LOWER(TRIM(b.name)) IN ('lost', 'new', 'quotation sent / báo giá', 'negotiation / đàm phán')
        THEN CAST(s.expected_close AS TIMESTAMP)
      WHEN LOWER(TRIM(b.name)) = 'signed / ký hợp đồng'
       AND COALESCE(CAST(s.custom_field.cf__fac_date AS TIMESTAMP), i.fac_date) IS NULL
        THEN COALESCE(i.sign_date, CAST(s.closed_date AS TIMESTAMP))
      WHEN LOWER(TRIM(b.name)) = 'signed / ký hợp đồng'
        THEN COALESCE(CAST(s.custom_field.cf__fac_date AS TIMESTAMP), i.fac_date)
      ELSE CAST(s.expected_close AS TIMESTAMP)
    END AS TIMESTAMP
  ) AS first_payment_date
FROM latest_deals s
INNER JOIN last_deal_stages b ON s.deal_stage_id = b.id
LEFT JOIN last_currencies lc  ON s.currency_id = lc.id
LEFT JOIN last_cm_contracts i  ON CAST(s.id AS STRING) = i.deal_id
LEFT JOIN user_team cu  ON CAST(s.owner_id AS STRING) = cu.am_user_id
LEFT JOIN last_sales_accounts sa  ON CAST(s.sales_account_id AS STRING) = sa.customer_id
LEFT JOIN last_cm_partners cp  ON COALESCE(NULLIF(TRIM(CAST(s.custom_field.cf_partner AS STRING)), ''), i.contract_partner_id) = cp.partner_id
LEFT JOIN crm_raw.internal_am_dictionary iad  ON LOWER(TRIM(sa.customer_alias)) = LOWER(TRIM(iad.company_alias))
LEFT JOIN crm_raw.territories territories  ON s.territory_id = territories.id
WHERE LOWER(TRIM(b.name)) <> 'lost'
"""
# =========================================================
# 3. EXECUTE
# =========================================================
df_deals = spark.sql(query)
# =========================================================
# 4. WRITE TARGET
# =========================================================
df_deals.write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)
spark.catalog.refreshTable(tgt_table)
# =========================================================
# 6. VALIDATE
# =========================================================
print("THANH CONG: Bang crm_silver.crm_mart_deals da duoc cap nhat.")