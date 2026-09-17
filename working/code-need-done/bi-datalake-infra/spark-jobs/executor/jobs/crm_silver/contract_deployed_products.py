# %livy.pyspark

sql_query = """
WITH last_cm_contracts AS (
    SELECT
        id,
        custom_field.cf_deployed_products AS cf_deployed_products
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
    trim(p)           AS deployed_product_name
FROM last_cm_contracts c
LATERAL VIEW explode(
    split(c.cf_deployed_products, ';')
) u AS p
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/contract_deployed_products"
    ) \
    .saveAsTable("crm_silver.contract_deployed_products")
