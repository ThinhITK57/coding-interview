%livy.pyspark

tgt_table = "bi_silver.crm_am_activity_daily"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_am_activity_daily"

# Làm mới danh mục file nguồn
spark.catalog.clearCache()

sql_query = """
WITH am_user_group AS (
  SELECT
    CAST(user_id AS STRING) AS am_user_id,

    CASE
      WHEN LOWER(TRIM(team_name)) = 'unnamed' AND LOWER(TRIM(job_title)) = 'am unnamed account'
    THEN 'UNNAMED'

    WHEN LOWER(TRIM(team_name)) = 'trọng điểm'  OR LOWER(TRIM(job_title)) = 'am vvip'
    THEN 'VVIP'

    WHEN LOWER(TRIM(job_title)) IN ('am kênh','am kinh doanh kênh' )
      OR LOWER(TRIM(team_name)) = 'kênh'
    THEN 'CHANNEL'

    WHEN LOWER(TRIM(job_title)) = 'am nội bộ'
       OR LOWER(TRIM(team_name)) = 'nội bộ'
    THEN 'INTERNAL'

    WHEN LOWER(TRIM(job_title)) IN ( 'am','bdm', 'global business development' )
      THEN 'INTERNATIONAL'
    ELSE 'OTHER'
    END AS am_group,

    CASE
      WHEN is_active = TRUE THEN 'ACTIVE'
      WHEN is_active = FALSE THEN 'INACTIVE'
      ELSE 'UNKNOWN'
    END AS am_status

  FROM bi_silver.crm_users
),

sales_account_base AS (
  SELECT
    CAST(sa.id AS STRING) AS customer_id,
    CAST(sa.am_user_id AS STRING) AS am_user_id,
    sa.am_username AS am_name,
    sa.name AS customer_name,
    sa.created_at,
    sa.updated_at
  FROM bi_silver.crm_sales_accounts sa
),

deal_base AS (
  SELECT
    CAST(d.deal_id AS STRING) AS opp_id,
    CAST(d.sales_account_id AS STRING) AS customer_id,
    COALESCE( CAST(d.am_user_id AS STRING), CAST(sa.am_user_id AS STRING) ) AS am_user_id,
    COALESCE( d.am_username, sa.am_username) AS am_name,
    COALESCE( d.company_name, sa.name  ) AS customer_name,

    d.deal_name AS opp_name,
    d.created_at,
    d.updated_at

  FROM bi_silver.crm_deals d
  LEFT JOIN bi_silver.crm_sales_accounts sa  ON CAST(d.sales_account_id AS STRING)  = CAST(sa.id AS STRING)
),

contract_base AS (
  SELECT
    CAST(c.id AS STRING) AS contract_id,
    CAST(c.deal_id AS STRING) AS opp_id,

    COALESCE(
    CAST(c.sale_account_id AS STRING),
    CAST(d.sales_account_id AS STRING)
    ) AS customer_id,

    COALESCE(
    CAST(c.user_am_id AS STRING),
    CAST(d.am_user_id AS STRING),
    CAST(sa.am_user_id AS STRING)
    ) AS am_user_id,

    COALESCE(
    d.am_username,
    sa.am_username
    ) AS am_name,

    COALESCE(
    c.customer_name,
    d.company_name,
    sa.name
    ) AS customer_name,

    d.deal_name AS opp_name,

    COALESCE(
    c.name,
    c.contract_number
    ) AS contract_name,

    c.created_at,
    c.updated_at

  FROM bi_silver.crm_contracts c
  LEFT JOIN bi_silver.crm_deals d   ON CAST(c.deal_id AS STRING) = CAST(d.deal_id AS STRING)
  LEFT JOIN bi_silver.crm_sales_accounts sa  ON COALESCE( CAST(c.sale_account_id AS STRING), CAST(d.sales_account_id AS STRING) ) = CAST(sa.id AS STRING)
),

customer_events AS (

  SELECT
    created_at AS report_date,
    am_user_id,
    am_name,
    customer_id,
    CAST(NULL AS STRING) AS opp_id,
    CAST(NULL AS STRING) AS contract_id,
    customer_name,
    CAST(NULL AS STRING) AS opp_name,
    CAST(NULL AS STRING) AS contract_name,
    'CUSTOMER_CREATED' AS activity_type
  FROM sales_account_base
  WHERE created_at IS NOT NULL

  UNION

  SELECT
    updated_at AS report_date,
    am_user_id,
    am_name,
    customer_id,
    CAST(NULL AS STRING) AS opp_id,
    CAST(NULL AS STRING) AS contract_id,
    customer_name,
    CAST(NULL AS STRING) AS opp_name,
    CAST(NULL AS STRING) AS contract_name,
    'CUSTOMER_UPDATED' AS activity_type
  FROM sales_account_base
  WHERE updated_at IS NOT NULL  AND (  created_at IS NULL  OR updated_at > created_at  )
),

deal_events AS (

  SELECT
    created_at AS report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    CAST(NULL AS STRING) AS contract_id,
    customer_name,
    opp_name,
    CAST(NULL AS STRING) AS contract_name,
    'OPPORTUNITY_CREATED' AS activity_type
  FROM deal_base
  WHERE created_at IS NOT NULL

  UNION

  SELECT
    updated_at AS report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    CAST(NULL AS STRING) AS contract_id,
    customer_name,
    opp_name,
    CAST(NULL AS STRING) AS contract_name,
    'OPPORTUNITY_UPDATED' AS activity_type
  FROM deal_base
  WHERE updated_at IS NOT NULL AND ( created_at IS NULL OR updated_at > created_at )
),

contract_events AS (

  SELECT
    created_at AS report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    contract_id,
    customer_name,
    opp_name,
    contract_name,
    'CONTRACT_CREATED' AS activity_type
  FROM contract_base
  WHERE created_at IS NOT NULL

  UNION

  SELECT
    updated_at AS report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    contract_id,
    customer_name,
    opp_name,
    contract_name,
    'CONTRACT_UPDATED' AS activity_type
  FROM contract_base
  WHERE updated_at IS NOT NULL   AND ( created_at IS NULL OR updated_at > created_at  )
),

all_events AS (

  SELECT
    report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    contract_id,
    customer_name,
    opp_name,
    contract_name,
    activity_type
  FROM customer_events

  UNION

  SELECT
    report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    contract_id,
    customer_name,
    opp_name,
    contract_name,
    activity_type
  FROM deal_events

  UNION

  SELECT
    report_date,
    am_user_id,
    am_name,
    customer_id,
    opp_id,
    contract_id,
    customer_name,
    opp_name,
    contract_name,
    activity_type
  FROM contract_events
)

SELECT
  a.report_date,
  a.am_user_id,
  a.am_name,

  COALESCE(
  u.am_group,
  'OTHER'
  ) AS am_group,

  COALESCE(
  u.am_status,
  'UNKNOWN'
  ) AS am_status,

  a.customer_id,
  a.opp_id,
  a.contract_id,
  a.customer_name,
  a.opp_name,
  a.contract_name,
  a.activity_type

FROM all_events a
LEFT JOIN am_user_group u ON a.am_user_id = u.am_user_id
"""

df = spark.sql(sql_query)

# 4) Save
df.repartition(1).write  \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)
  
# .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
# 5) Quick check
spark.sql("SELECT * FROM " + tgt_table + " LIMIT 1").show(truncate=False)
