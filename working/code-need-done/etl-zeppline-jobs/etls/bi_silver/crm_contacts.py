# %livy.pyspark
tgt_table = "bi_silver.crm_contacts"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_contacts"


import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())

# Làm mới danh mục file nguồn
spark.sql("REFRESH TABLE crm_raw.contacts")
spark.catalog.clearCache()
sql_query = """
WITH last_contacts AS (
  SELECT *
  FROM (
    SELECT
      ss.*,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
      ) rn
    FROM crm_raw.contacts ss
  ) t
  WHERE rn = 1
)
SELECT
  CAST(c.id AS STRING) AS id,
  c.display_name,
  c.last_contacted_sales_activity_mode,
  
  CAST(c.owner_id AS STRING) as user_am_id,
  uu.display_name as user_am_name,
  
  CAST(c.sales_account_id AS STRING) as sales_account_id,
  cst.name as sales_account_name,
  
  c.contact_status_id as contact_status_id,
  cst.name as contact_status_name,
  cst.forecast_type as contact_forecast_type,
  
  
  c.custom_field.cf__am as am_fullname,
  c.custom_field.cf_warm_up_source as warm_up_source,
  c.custom_field.cf_initial_source as initial_source,
  c.custom_field.cf_center as center_name,
  c.custom_field.cf_level_of_support as level_of_support,
  c.custom_field.cf_level_of_interest as level_of_interest,
  c.custom_field.cf__company as company_name,
  c.custom_field.cf_partner as partner_name,
  c.custom_field.cf_presales as presales_name,
  c.custom_field.cf_job_title as job_title
  
  , CAST(last_assigned_at as TIMESTAMP) AS last_assigned_at
  , CAST(last_contacted as TIMESTAMP) AS last_contacted_at
  , CAST(last_contacted_via_sales_activity as TIMESTAMP) AS last_contacted_via_sales_activity
  , CAST(created_at as TIMESTAMP) AS created_at
  , CAST(updated_at as TIMESTAMP) AS updated_at

FROM last_contacts c
LEFT JOIN bi_silver.crm_users uu ON c.owner_id = uu.user_id
LEFT JOIN crm_raw.contact_statuses cst ON c.sales_account_id = cst.id
"""

df = spark.sql(sql_query)


# 4) Save
df.repartition(1).write  \
  .mode("overwrite") \
  .format("parquet") \
 .option("path", tgt_path) \
 .saveAsTable(tgt_table)

# .save(tgt_path)
# .option("path", tgt_path) \
# .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)