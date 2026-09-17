# %livy.pyspark
tgt_table = "bi_silver.crm_sales_accounts"
tgt_path  = "s3a://bi-silver/crm_sales_accounts"

spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE crm_raw.sales_accounts")
spark.sql("REFRESH TABLE crm_raw.deleted_sales_accounts")
spark.sql("REFRESH TABLE crm_raw.business_types")
spark.sql("REFRESH TABLE crm_raw.industry_types")

# ------------------------------------------------------------------
# 1. LẤY SALES ACCOUNT MỚI NHẤT (ANTI-JOIN DELETED)
# ------------------------------------------------------------------
df_partners = spark.sql("""
SELECT
    parent_sales_account_id as parent_id,
    true as is_partner
FROM crm_raw.sales_accounts ss
WHERE parent_sales_account_id is not null and NOT EXISTS (
    SELECT 1
    FROM crm_raw.deleted_sales_accounts d
    WHERE CAST(d.id AS BIGINT) = ss.id
)
""")

df_partners.createOrReplaceTempView("sales_partners_view")


query = """
WITH ranked_accounts AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.id
      ORDER BY s.updated_at DESC, s.created_at DESC
    ) AS rank_account
  FROM crm_raw.sales_accounts s
  WHERE NOT EXISTS (
      SELECT 1
      FROM crm_raw.deleted_sales_accounts d
      WHERE CAST(d.id AS BIGINT) = s.id
  )
),
latest_accounts AS (
  SELECT * FROM ranked_accounts WHERE rank_account = 1
)
SELECT
  CAST(s.id AS STRING) AS id,
  coalesce(pn.is_partner, false) as is_partner,
  COALESCE(s.name, 'N/A') as name,
  COALESCE(s.address, 'N/A') as address,
  COALESCE(s.city, 'N/A') as city,
  COALESCE(s.state, 'N/A') as state,
  COALESCE(s.zipcode, 'N/A') as zipcode,
  COALESCE(s.country, 'N/A') as country,
  COALESCE(s.number_of_employees, -1) as number_of_employees,
  COALESCE(s.annual_revenue, 0) as annual_revenue,
  COALESCE(s.website, 'N/A') as website,

  COALESCE(s.custom_field.cf__am, 'N/A') AS am_username,
  COALESCE(s.custom_field.cf__am, 'N/A') AS sales_admin,

  CAST(COALESCE(s.phone, 'N/A') AS STRING) AS phone,
  CAST(COALESCE(s.open_deals_amount, 0) AS DECIMAL(18,2)) AS open_deals_amount,
  CAST(COALESCE(s.open_deals_count, 0)  AS BIGINT)        AS open_deals_count,
  CAST(COALESCE(s.won_deals_amount, 0) AS DECIMAL(18,2)) AS won_deals_amount,
  CAST(COALESCE(s.won_deals_count, 0)   AS BIGINT)        AS won_deals_count,

  COALESCE(s.custom_field.cf_tax_code, 'N/A') AS tax_code,

  sha2(lower(trim(CAST(s.custom_field.cf_alias AS STRING))), 256) AS company_id,
  COALESCE(s.custom_field.cf_alias, 'N/A') AS company_alias,

  COALESCE(s.custom_field.cf_segment, 'N/A')  AS customer_segment_l1,
  COALESCE(s.custom_field.cf_segment2, '-')   AS customer_segment_l2,
    
  COALESCE(s.custom_field.cf_segment3, 'N/A' ) AS customer_segment_l3,

  TO_TIMESTAMP(TO_DATE(s.custom_field.cf_incorporation_date, 'yyyy-MM-dd')) AS incorporation_date,

  COALESCE(s.custom_field.cf_initial_source, 'N/A') AS initial_source,
  COALESCE(s.custom_field.cf_warm_up_source, 'N/A') AS warm_up_source,

  COALESCE(s.custom_field.cf_using_soc, 'N/A')       AS using_soc,
  COALESCE(s.custom_field.cf_vcs_socothers, 'N/A')   AS vcs_soc_others,
  COALESCE(s.custom_field.cf_soc_brand, 'N/A')       AS soc_brand,
  COALESCE(s.custom_field.cf_service_level, 'N/A')   AS service_level,

  s.last_contacted_mode AS last_contacted_mode,
  CAST(s.last_contacted as TIMESTAMP) AS last_contacted_at,
  CAST(s.created_at as TIMESTAMP) AS created_at,
  CAST(s.updated_at as TIMESTAMP) AS updated_at,
  DATEDIFF(CURRENT_DATE(), TO_DATE(s.updated_at)) AS days_since_update,

  CAST(s.parent_sales_account_id as STRING) as parent_sales_account_id,
  CASE
    WHEN s.recent_note IS NULL THEN 'None'
    ELSE CAST(s.recent_note AS STRING)
  END AS recent_note,
  TO_TIMESTAMP(TO_DATE(s.last_contacted_via_sales_activity)) AS last_contacted_via_sales_activity,
  s.last_contacted_sales_activity_mode,
  TO_TIMESTAMP(TO_DATE(s.last_assigned_at)) AS last_assigned_at,

  TO_TIMESTAMP(TO_DATE(s.renewal_date)) AS renewal_date,

  COALESCE(b.name, 'N/A') AS business_type,
  COALESCE(j.name, 'N/A') AS industry_type
  , s.owner_id AS am_user_id
  , uu.display_name as user_am_name

FROM latest_accounts s
LEFT JOIN crm_raw.business_types b ON s.business_type_id = b.id
LEFT JOIN crm_raw.industry_types j ON s.industry_type_id = j.id
LEFT JOIN sales_partners_view pn ON pn.parent_id = s.id
LEFT JOIN bi_silver.crm_users uu ON s.owner_id = uu.user_id

WHERE s.is_deleted = false
"""

df = spark.sql(query)

# 1) Drop table (KHÔNG dùng f-string để tránh lỗi Python2)
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

# 3) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)

# .save(tgt_path)
# .option("path", tgt_path) \
# .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

spark.catalog.dropTempView("sales_partners_view")
