%livy.pyspark

sql_query = """
WITH last_sales_accounts AS (
    SELECT *
    FROM (
        SELECT
            ss.*,
            ROW_NUMBER() OVER (
                PARTITION BY ss.id
                ORDER BY ss.updated_at_ts DESC, ss.id DESC
            ) AS rn
        FROM crm_raw.sales_accounts ss
        LEFT ANTI JOIN crm_raw.deleted_sales_accounts d
            ON CAST(d.id AS BIGINT) = ss.id
    ) t
    WHERE rn = 1
)

SELECT
    CAST(s.id AS STRING)        AS sale_id,
    TRIM(p)                     AS interested_product
FROM last_sales_accounts s
LATERAL VIEW explode(
    split(
        COALESCE(s.custom_field.cf_interested_products, ''),
        ';'
    )
) exploded AS p
WHERE s.is_deleted = false
  AND TRIM(p) <> ''
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "/opt/datasets/crawlers/vcs_silver/crm_silver/data/sales_account_interested_products"
    ) \
    .saveAsTable("crm_silver.sales_account_interested_products")
