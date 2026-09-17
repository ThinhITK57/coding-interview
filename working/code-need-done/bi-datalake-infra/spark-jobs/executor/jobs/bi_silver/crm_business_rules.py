# %livy.pyspark

tgt_table = "bi_silver.crm_business_rules"
tgt_path  = "s3a://bi-silver/crm_business_rules"

# Làm mới danh mục file nguồn
spark.sql("REFRESH TABLE crm_raw.cm_business_rules")
spark.catalog.clearCache()

sql_query = """
WITH last_cm_business_rules AS (
  SELECT *
  FROM (
    SELECT
      ss.*,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, created_at_ts DESC, crawled_at_ts DESC
      ) rn
    FROM crm_raw.cm_business_rules ss
  ) t
  WHERE rn = 1
)

SELECT
  CAST(c.id AS STRING) AS id,
  TRIM(c.name) AS business_rule_name,

  CAST(c.owner_id AS STRING) AS am_user_id,

  TRIM(c.custom_field.cf_type) AS rule_type,
  TRIM(c.custom_field.cf_price_type) AS price_type,

  CAST(c.custom_field.cf_parent_product AS STRING) AS parent_product_id,
  CAST(c.custom_field.cf_child_product AS STRING) AS child_product_id,

  UPPER(TRIM(c.custom_field.cf_ratio_operator)) AS ratio_operator,
  CAST(c.custom_field.cf_ratio_value AS BIGINT) AS ratio_value,
  TRIM(c.custom_field.cf_ratio_unit) AS ratio_unit,

  COALESCE(c.custom_field.cf_active, false) AS is_active,

  CAST(c.created_at AS TIMESTAMP) AS created_at,
  CAST(c.creator_id AS STRING) AS creator_id,
  CAST(c.updated_at AS TIMESTAMP) AS updated_at,
  CAST(c.updater_id AS STRING) AS updater_id,
  c.recent_note,
  CAST(c.record_type_id AS STRING) AS record_type_id


FROM last_cm_business_rules c
"""

df = spark.sql(sql_query)

# 4) Save
df.repartition(1).write  \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
  # .option("path", tgt_path) \
  # .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
# 5) Quick check
spark.sql("SELECT * FROM " + tgt_table + " LIMIT 1").show(truncate=False)
