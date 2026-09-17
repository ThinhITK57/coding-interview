%livy.pyspark

target_table = "bi_silver.cx_lifecycle_customer_product_snapshot"
target_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/cx_lifecycle_customer_product_snapshot"

# 3. Clean up existing target to prevent metadata mismatch
spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.dim_date")
spark.sql("REFRESH TABLE bi_silver.crm_committed_revenue")
spark.sql("REFRESH TABLE bi_silver.crm_contracts")

sql_query = """
WITH customer_status AS (

    SELECT
        year,
        quarter,
        month,
        period_start,
        period_end,

        tax_code,
        product_category,

        MAX(is_active)               AS is_active,
        MAX(is_expiring_90d)         AS is_expiring_90d,
        MAX(is_grace_period)         AS is_grace_period,
        MAX(is_new_customer_vcs)     AS is_new_customer_vcs,
        MAX(is_new_customer_product) AS is_new_customer_product,
        MAX(is_renewal)              AS is_renewal,
        MAX(is_upsell)               AS is_upsell,
        MAX(is_returning_customer)   AS is_returning_customer,

        MAX(expire_date)             AS latest_expire_date,
        MAX(is_next_signed_within_90_days) AS is_next_signed_within_90_days

    FROM bi_silver.cx_cso_customer_contract_lifecycle_snapshot

    GROUP BY
        year,
        quarter,
        month,
        period_start,
        period_end,
        tax_code,
        product_category
)

SELECT

    year,
    quarter,
    month,
    period_start,
    period_end,
    
    tax_code,
    product_category,

    is_active,
    is_expiring_90d,
    is_grace_period,
    is_new_customer_vcs,
    is_new_customer_product,
    is_renewal,
    is_upsell,
    is_returning_customer,

    latest_expire_date,

    CASE
        WHEN is_active = 0
         AND datediff(period_end, latest_expire_date) > 90     --(solution 1)
         --AND is_next_signed_within_90_days = 0                --(solution 2)
        THEN 1
        ELSE 0
    END AS is_churned

FROM customer_status
"""


df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
#   .save(target_path)
)
spark.catalog.refreshTable(target_table)
print(target_table)