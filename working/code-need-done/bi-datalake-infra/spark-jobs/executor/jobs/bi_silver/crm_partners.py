# %livy.pyspark
spark.catalog.clearCache()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.sql("REFRESH TABLE crm_raw.cm_partners")

tgt_table = "bi_silver.crm_partners"
tgt_path  = "s3a://bi-silver/crm_partners"

sql_query = """
WITH ranked AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (
      PARTITION BY p.id
      ORDER BY p.updated_at_ts DESC, p.id DESC
    ) AS rn
  FROM crm_raw.cm_partners p
),
last_cm_partners AS (
  SELECT * FROM ranked WHERE rn = 1
)
SELECT
  CAST(p.id AS STRING) AS id,
  p.name as partner_name,

  COALESCE(p.custom_field.cf_document_number, 'N/A') AS document_number,
  COALESCE(p.custom_field.cf_alias, 'N/A')           AS company_alias,
  COALESCE(p.custom_field.cf_address, 'N/A')         AS partner_address,
  COALESCE(p.custom_field.cf_country, 'N/A')         AS country,
  COALESCE(p.custom_field.cf_contact_name, 'N/A')    AS contact_name,
  COALESCE(p.custom_field.cf_contact_position, 'N/A') AS contact_position,
  COALESCE(p.custom_field.cf_contact_mobile, 'N/A')  AS contact_mobile,
  COALESCE(p.custom_field.cf_contact_email, 'N/A')   AS contact_email,

  TRY_CAST(p.custom_field.cf_effective_date  AS TIMESTAMP) AS effective_date,
  TRY_CAST(p.custom_field.cf_expiration_date AS TIMESTAMP) AS expiration_date,

  -- Parse EN/VI dạng "en/vi" (Spark2 không có element_at)
  CASE
    WHEN p.custom_field.cf_type IS NULL or p.custom_field.cf_type = '' THEN 'N/A'
    ELSE trim(p.custom_field.cf_type)
  END AS partner_type,
  CASE
    WHEN p.custom_field.cf_document_format IS NULL or p.custom_field.cf_document_format = '' THEN 'N/A'
    ELSE trim(p.custom_field.cf_document_format)
  END AS document_type,
  CASE
    WHEN p.custom_field.cf_status IS NULL or p.custom_field.cf_status = '' THEN 'N/A'
    ELSE trim(p.custom_field.cf_status)
  END AS partner_status,

  CASE
    WHEN p.custom_field.cf_type IS NULL or p.custom_field.cf_type = '' THEN 'N/A'
    WHEN size(split(p.custom_field.cf_type, '/')) >= 2 THEN trim(split(p.custom_field.cf_type, '/')[1])
    ELSE NULL
  END AS partner_type_vi,
  CASE
    WHEN p.custom_field.cf_document_format IS NULL or p.custom_field.cf_document_format = '' THEN 'N/A'
    WHEN size(split(p.custom_field.cf_document_format, '/')) >= 2 THEN trim(split(p.custom_field.cf_document_format, '/')[1])
    ELSE NULL
  END AS document_format_vi,
  CASE
    WHEN p.custom_field.cf_status IS NULL THEN 'N/A'
    WHEN size(split(p.custom_field.cf_status, '/')) >= 2 THEN trim(split(p.custom_field.cf_status, '/')[1])
    ELSE NULL
  END AS partner_status_vi

FROM last_cm_partners p
"""

df = spark.sql(sql_query)

# # Drop table + hard delete path để CHỐNG DOUBLE
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)


# Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
  # .option("path", tgt_path) \
  # .saveAsTable(tgt_table)


spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())
