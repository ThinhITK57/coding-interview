%livy.pyspark

sql_query = """
WITH last_cm_contracts AS (
    SELECT
        id,
        custom_field.cf_products_in_contract AS cf_products_in_contract
    FROM (
        SELECT
            id,
            custom_field,
            updated_at_ts,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at_ts DESC
            ) AS rn
        FROM crm_raw.cm_contracts
    ) t
    WHERE rn = 1
)

SELECT
    c.id              AS contract_id,
    trim(p)           AS signed_product_name
FROM last_cm_contracts c
LATERAL VIEW explode(
    split(c.cf_products_in_contract, ';')
) u AS p
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "/opt/datasets/crawlers/vcs_silver/crm_silver/data/contract_signed_products"
    ) \
    .saveAsTable("crm_silver.contract_signed_products")
