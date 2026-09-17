%livy.pyspark

sql_query = """
SELECT
    id,
    name,
    number_of_employees,
    annual_revenue,
    am_username,
    open_deals_amount,
    open_deals_count,
    won_deals_amount,
    won_deals_count,
    tax_code,
    company_id,

    CAST(incorporation_date AS TIMESTAMP) AS incorporation_date,
    initial_source,
    warm_up_source,
    using_soc,
    vcs_soc_others,
    soc_brand,
    service_level,

    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    days_since_update,
    parent_sales_account_id,
    recent_note,

    CAST(last_contacted_via_sales_activity AS TIMESTAMP) AS last_contacted_via_sales_activity,
    last_contacted_sales_activity_mode,

    CAST(last_assigned_at AS TIMESTAMP) AS last_assigned_at,
    CAST(renewal_date AS TIMESTAMP)     AS renewal_date,

    business_type,
    industry_type,
    is_vip
FROM bi_silver.crm_sales_accounts
"""

df = spark.sql(sql_query)

# =========================
# Target config
# =========================
target_table = "bi_gold.crm_sales_accounts"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_sales_accounts"

# =========================
# 1) Drop table metadata
# =========================
#spark.sql(f"DROP TABLE IF EXISTS {target_table}")

# =========================
# 2) Hard delete path
# =========================
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)

# if fs.exists(path):
#     fs.delete(path, True)

# =========================
# 3) Write parquet + save table
# =========================
(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

# print(f"DONE: {target_table} saved at {target_path}")
