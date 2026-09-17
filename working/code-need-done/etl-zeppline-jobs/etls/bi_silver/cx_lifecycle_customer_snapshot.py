%livy.pyspark

target_table = "bi_silver.cx_lifecycle_customer_snapshot"
target_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/cx_lifecycle_customer_snapshot"

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

        --------------------------------------------------------------------
        -- Status
        --------------------------------------------------------------------
        MAX(is_active)               AS is_active,
        MAX(is_expiring_90d)         AS is_expiring_90d,
        MAX(is_grace_period)         AS is_grace_period,
        MAX(is_new_customer_vcs)     AS is_new_customer,
        MAX(is_renewal)              AS is_renewal,
        MAX(is_upsell)               AS is_upsell,
        MAX(is_returning_customer)   AS is_returning_customer,
        MAX(is_next_signed_within_90_days) AS is_next_signed_within_90_days,

        --------------------------------------------------------------------
        -- Contract summary
        --------------------------------------------------------------------
        MAX(expire_date)             AS latest_expire_date,
        MAX(sign_date)               AS latest_sign_date,

        COUNT(*)                     AS total_contract,
        COUNT(DISTINCT product_category) AS total_product

    FROM bi_silver.cx_cso_customer_contract_lifecycle_snapshot

    GROUP BY
        year,
        quarter,
        month,
        period_start,
        period_end,
        tax_code

)

SELECT
    year,
    quarter,
    month,
    period_start,
    period_end,
    
    tax_code,

    is_active,
    is_expiring_90d,
    is_grace_period,
    is_new_customer,
    is_renewal,
    is_upsell,
    is_returning_customer,

    latest_expire_date,
    latest_sign_date,

    total_contract,
    total_product,

    --------------------------------------------------------------------
    -- Churn
    --------------------------------------------------------------------
    CASE
        WHEN is_active = 0
            AND is_next_signed_within_90_days = 0
        THEN 1
        ELSE 0
    END AS is_churned,

    --------------------------------------------------------------------
    -- Lifecycle
    --------------------------------------------------------------------
    CASE
        WHEN is_expiring_90d = 1 THEN 'EXPIRING_90D'
        WHEN is_active = 1 THEN 'ACTIVE'
        WHEN is_grace_period = 1 THEN 'GRACE_PERIOD'
        WHEN is_active = 0
            AND datediff(period_end, latest_expire_date) > 90
            THEN 'CHURNED'
        ELSE 'INACTIVE'
    END AS customer_lifecycle

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