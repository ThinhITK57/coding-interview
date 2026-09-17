# Tạm hide do vấn đề bảo mật dữ liệu cá nhân
# Update 22/4: Rollback bảng thông tin, mask dữ liệu cá nhân ****
# %livy.pyspark

spark.sql("REFRESH TABLE crm_raw.contacts")
spark.sql("REFRESH TABLE cx_cso_raw.dim_cso_contacts")
spark.catalog.clearCache()

tgt_table = "bi_silver.crm_company_contacts"
tgt_path  = "s3a://bi-silver/crm_company_contacts"

sql_query = """

WITH cso_company_contacts AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(company_id AS STRING)), ''), 'unknown') AS company_id,
        COALESCE(NULLIF(TRIM(company.name), ''), 'unknown') AS company_name,

        'cso' AS source_system,

        CAST(id AS STRING) AS contact_id,

        COALESCE(
            NULLIF(TRIM(name), ''),
            NULLIF(TRIM(CONCAT_WS(' ', first_name, last_name)), '')
        ) AS contact_name,
        NULLIF(TRIM(first_name), '') AS first_name,
        NULLIF(TRIM(last_name), '') AS last_name,

         -- Mask email
        CASE
            WHEN NULLIF(TRIM(email), '') IS NULL THEN NULL
            WHEN POSITION('@' IN TRIM(email)) > 1 THEN
                CONCAT(
                    SUBSTR(TRIM(email), 1, 1),
                    REPEAT('*', GREATEST(POSITION('@' IN TRIM(email)) - 2, 1)),
                    SUBSTR(TRIM(email), POSITION('@' IN TRIM(email)))
                )
            ELSE '***'
        END AS email,
        CASE
            WHEN email IS NOT NULL AND POSITION('@' IN TRIM(email)) > 0
                THEN SPLIT(TRIM(email), '@')[2]
            ELSE NULL
        END AS email_domain,

        CAST(from_unixtime(CAST(created_at_ts / 1000 AS BIGINT)) AS TIMESTAMP) AS created_at,
        CAST(from_unixtime(CAST(updated_at_ts / 1000 AS BIGINT)) AS TIMESTAMP) AS updated_at

    FROM cx_cso_raw.dim_cso_contacts
    WHERE id IS NOT NULL
      AND NULLIF(TRIM(CAST(id AS STRING)), '') IS NOT NULL
),

crm_company_contacts AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(sales_account_id AS STRING)), ''), 'unknown') AS company_id,
        COALESCE(NULLIF(TRIM(custom_field.cf__company), ''), 'unknown') AS company_name,

        'crm' AS source_system,

        CAST(id AS STRING) AS contact_id,

        COALESCE(
            NULLIF(TRIM(display_name), ''),
            NULLIF(TRIM(CONCAT_WS(' ', first_name, last_name)), '')
        ) AS contact_name,
        NULLIF(TRIM(first_name), '') AS first_name,
        NULLIF(TRIM(last_name), '') AS last_name,

         -- Mask email
        CASE
            WHEN NULLIF(TRIM(email), '') IS NULL THEN NULL
            WHEN POSITION('@' IN TRIM(email)) > 1 THEN
                CONCAT(
                    SUBSTR(TRIM(email), 1, 1),
                    REPEAT('*', GREATEST(POSITION('@' IN TRIM(email)) - 2, 1)),
                    SUBSTR(TRIM(email), POSITION('@' IN TRIM(email)))
                )
            ELSE '***'
        END AS email,
        CASE
            WHEN email IS NOT NULL AND POSITION('@' IN TRIM(email)) > 0
                THEN SPLIT(TRIM(email), '@')[2]
            ELSE NULL
        END AS email_domain,

        to_timestamp(SUBSTR(created_at, 1, 19), "yyyy-MM-dd'T'HH:mm:ss") AS created_at,
        to_timestamp(SUBSTR(updated_at, 1, 19), "yyyy-MM-dd'T'HH:mm:ss") AS updated_at

    FROM crm_raw.contacts
    WHERE id IS NOT NULL
      AND NULLIF(TRIM(CAST(id AS STRING)), '') IS NOT NULL
),

all_contacts AS (
    SELECT * FROM cso_company_contacts
    UNION ALL
    SELECT * FROM crm_company_contacts
),

ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY contact_id
               ORDER BY updated_at DESC NULLS LAST
           ) AS rn
    FROM all_contacts
)

SELECT 
company_id,
contact_id,
company_name,
created_at,
contact_name,
email,
source_system,
updated_at

FROM ranked
WHERE rn = 1

"""

df = spark.sql(sql_query)

df = df.dropDuplicates(["company_id","contact_id"])

df.write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print("Rows:", df.count())