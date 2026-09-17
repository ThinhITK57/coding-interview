%livy.pyspark

target_table = "bi_silver.cx_lifecycle_customer_contract_snapshot"
target_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/cx_lifecycle_customer_contract_snapshot"


# 3. Clean up existing target to prevent metadata mismatch
spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.dim_date")
spark.sql("REFRESH TABLE bi_silver.crm_committed_revenue")
spark.sql("REFRESH TABLE bi_silver.crm_contracts")


customer_sign_src = spark.sql("""
SELECT DISTINCT
    c.tax_code,
    ar.product_category,
    CAST(ar.sign_date AS DATE) AS sign_date,
    CAST(ar.contract_expire_date AS DATE) AS expire_date,
    c.contract_type,

    ----------------------------------------------------------------
    -- Month
    ----------------------------------------------------------------
    trunc(ar.sign_date,'MM') AS sign_date_month_start,
    last_day(ar.sign_date) AS sign_date_month_end,

    trunc(ar.contract_expire_date,'MM') AS expire_date_month_start,
    last_day(ar.contract_expire_date) AS expire_date_month_end,

    ----------------------------------------------------------------
    -- Quarter
    ----------------------------------------------------------------
    trunc(ar.contract_expire_date,'Q') AS expire_date_quarter_start,

    date_add(
        add_months(trunc(ar.contract_expire_date,'Q'),3),
        -1
    ) AS expire_date_quarter_end,

    ----------------------------------------------------------------
    -- Latest expire
    ----------------------------------------------------------------
    MAX(CAST(ar.contract_expire_date AS DATE))
        OVER (
            PARTITION BY c.tax_code
        ) AS latest_expire_date_tax,

    MAX(CAST(ar.contract_expire_date AS DATE))
        OVER (
            PARTITION BY c.tax_code, ar.product_category
        ) AS latest_expire_date_product,

    ----------------------------------------------------------------
    -- Previous contract
    ----------------------------------------------------------------
    lag(CAST(ar.sign_date AS DATE))
        OVER (
            PARTITION BY c.tax_code, ar.product_category
            ORDER BY ar.sign_date
        ) AS prev_sign_date_same_product,

    lag(CAST(ar.sign_date AS DATE))
        OVER (
            PARTITION BY c.tax_code
            ORDER BY ar.sign_date
        ) AS prev_sign_date_tax,

    lag(ar.product_category)
        OVER (
            PARTITION BY c.tax_code
            ORDER BY ar.sign_date
        ) AS prev_product_category,

    ----------------------------------------------------------------
    -- First purchase
    ----------------------------------------------------------------
    row_number()
        OVER (
            PARTITION BY c.tax_code
            ORDER BY ar.sign_date
        ) AS rn_tax,

    row_number()
        OVER (
            PARTITION BY c.tax_code, ar.product_category
            ORDER BY ar.sign_date
        ) AS rn_product,

    ----------------------------------------------------------------
    -- Last expire
    ----------------------------------------------------------------
    MAX(CAST(ar.contract_expire_date AS DATE))
        OVER (
            PARTITION BY c.tax_code
        ) AS latest_expire_date

FROM bi_silver.crm_committed_revenue ar
JOIN bi_silver.crm_contracts c
    ON ar.deal_id = c.deal_id
""")

customer_sign_src.createOrReplaceTempView("customer_sign_src")

parameter_date_src = spark.sql("""
SELECT DISTINCT
    last_day(date_add('2014-01-01', CAST(id * 5 AS INT) )) AS parameter_date
FROM range(0, 1700)
WHERE date_add('2014-01-01', CAST(id * 5 AS INT) ) <= '2036-12-31'
""")

parameter_date_src.createOrReplaceTempView("parameter_date_src")


sql_query = """
SELECT

    p.parameter_date as report_date,

    c.tax_code,
    c.product_category,
    c.contract_type,

    c.sign_date,
    c.expire_date,

    c.prev_sign_date_same_product,
    c.prev_sign_date_tax,
    c.prev_product_category,

    c.rn_tax,
    c.rn_product,

    c.latest_expire_date_tax,
    c.latest_expire_date_product,

    --------------------------------------------------------------------
    -- Contract đã phát sinh tại thời điểm snapshot
    --------------------------------------------------------------------
    CASE
        WHEN c.sign_date <= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_signed,

    --------------------------------------------------------------------
    -- Active
    --------------------------------------------------------------------
    CASE
        WHEN c.sign_date <= p.parameter_date
         AND c.expire_date >= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_active,

    --------------------------------------------------------------------
    -- Expiring 90 ngày
    --------------------------------------------------------------------
    CASE
        WHEN c.sign_date <= p.parameter_date
         AND c.expire_date BETWEEN p.parameter_date
                               AND date_add(p.parameter_date, 90)
        THEN 1
        ELSE 0
    END AS is_expiring_90d,

    --------------------------------------------------------------------
    -- Grace period
    --------------------------------------------------------------------
    CASE
        WHEN c.expire_date < p.parameter_date
         AND datediff(p.parameter_date, c.expire_date) <= 90
        THEN 1
        ELSE 0
    END AS is_grace_period,

    --------------------------------------------------------------------
    -- New Customer VCS
    --------------------------------------------------------------------
    CASE
        WHEN c.rn_tax = 1
         AND c.sign_date <= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_new_customer_vcs,

    --------------------------------------------------------------------
    -- New Product
    --------------------------------------------------------------------
    CASE
        WHEN c.rn_product = 1
         AND c.sign_date <= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_new_customer_product,

    --------------------------------------------------------------------
    -- Renewal
    --------------------------------------------------------------------
    CASE
        WHEN c.prev_sign_date_same_product IS NOT NULL
         AND c.sign_date <= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_renewal,

    --------------------------------------------------------------------
    -- Upsell
    --------------------------------------------------------------------
    CASE
        WHEN c.prev_sign_date_tax IS NOT NULL
         AND c.prev_product_category <> c.product_category
         AND c.sign_date <= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_upsell,

    --------------------------------------------------------------------
    -- Returning
    --------------------------------------------------------------------
    CASE
        WHEN c.prev_sign_date_tax IS NOT NULL
         AND datediff(c.sign_date, c.prev_sign_date_tax) > 90
         AND c.sign_date <= p.parameter_date
        THEN 1
        ELSE 0
    END AS is_returning_customer

FROM customer_sign_src c
CROSS JOIN parameter_date_src p
WHERE p.parameter_date >= c.sign_date and p.parameter_date <= c.expire_date
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