%livy.pyspark

target_table = "bi_silver.cx_cso_customer_contract_lifecycle_snapshot"
target_path  = "/opt/datasets/crawlers/vcs/bi_silver/data/cx_cso_customer_contract_lifecycle_snapshot"

# 3. Clean up existing target to prevent metadata mismatch
spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.dim_date")
spark.sql("REFRESH TABLE bi_silver.crm_committed_revenue")

# 4. Core Transformation Logic
# Added 'r' prefix here to preserve raw strings for regex patterns safely
contract_src= spark.sql('''
    SELECT DISTINCT
            c.tax_code,
            ar.product_category,
            ar.customer_name,
            ar.product_type,
            CAST(ar.sign_date AS DATE) AS sign_date,
            CAST(ar.contract_expire_date AS DATE) AS expire_date
        FROM bi_silver.crm_committed_revenue ar
        JOIN bi_silver.crm_contracts c
            ON ar.deal_id = c.deal_id
        --WHERE c.tax_code = '0100109106-010' --DEBUG DATA
'''
)
contract_src.createOrReplaceTempView("contract_src")


contract_timeline = spark.sql('''
WITH prev_next_sign AS (
    SELECT
        concat_ws(
            '|',
            tax_code,
            product_category,
            CAST(sign_date AS STRING),
            CAST(expire_date AS STRING)
        ) AS concatenated_info,

        sign_date,
        expire_date,

        LEAD(sign_date) OVER (
            PARTITION BY tax_code, product_category
            ORDER BY sign_date
        ) AS next_sign,

        LAG(expire_date) OVER (
            PARTITION BY tax_code, product_category
            ORDER BY sign_date
        ) AS prev_expire

    FROM (
        SELECT DISTINCT 
            tax_code, product_category, sign_date, expire_date
        FROM contract_src)
)
SELECT
    concatenated_info,
    --next_sign,
    --prev_expire,

    CASE
        WHEN next_sign IS NOT NULL
         AND next_sign BETWEEN sign_date AND date_add(expire_date, 90)
        THEN 1
        ELSE 0
    END AS is_next_signed_within_90_days,

    CASE
        WHEN prev_expire IS NOT NULL
         AND datediff(sign_date, prev_expire) > 90
        THEN 1
        ELSE 0
    END AS is_returning_customer

FROM prev_next_sign

''')
contract_timeline.createOrReplaceTempView("contract_timeline")


customer_product_lifecycle_base_src = spark.sql('''
    WITH contract_base AS (
        SELECT DISTINCT
            cs.tax_code,
            cs.customer_name,
            LOWER(cs.product_type) AS contract_type,
            cs.product_category,
            CAST(cs.sign_date AS DATE) AS sign_date,
            CAST(cs.expire_date AS DATE) AS expire_date,
            ctl.is_next_signed_within_90_days,
            ctl.is_returning_customer                            
        FROM contract_src cs
        INNER JOIN contract_timeline ctl
            ON CONCAT_WS(
                '|',
                cs.tax_code,
                cs.product_category,
                CAST(CAST(cs.sign_date AS DATE) AS STRING),
                CAST(CAST(cs.expire_date AS DATE) AS STRING)
            ) = ctl.concatenated_info
    ),

    first_customer_sign AS (
        SELECT
            tax_code,
            MIN(sign_date) AS first_customer_sign_date
        FROM contract_base
        GROUP BY tax_code
    ),

    first_customer_product_sign AS (
        SELECT
            tax_code,
            product_category,
            MIN(sign_date) AS first_customer_product_sign_date
        FROM contract_base
        GROUP BY
            tax_code,
            product_category
    )

    SELECT
        cb.tax_code,
        cb.customer_name,
        cb.contract_type,
        cb.product_category,
        cb.sign_date,
        cb.expire_date,
        cb.is_next_signed_within_90_days,
        cb.is_returning_customer,
        fcs.first_customer_sign_date,
        fcps.first_customer_product_sign_date
    FROM contract_base cb
    LEFT JOIN first_customer_sign fcs
        ON cb.tax_code = fcs.tax_code
    LEFT JOIN first_customer_product_sign fcps
        ON cb.tax_code = fcps.tax_code
    AND cb.product_category = fcps.product_category
''')
customer_product_lifecycle_base_src.createOrReplaceTempView("customer_product_lifecycle_base_src")


customer_snapshot_month_src = spark.sql('''
    WITH dim_month AS (
        SELECT
            date_year AS year,
            date_quarter AS quarter,
            date_month AS month,
            MIN(date_value) AS period_start,
            MAX(date_value) AS period_end
        FROM bi_silver.dim_date
        WHERE date_year BETWEEN 2010 AND 2037
        GROUP BY
            date_year,
            date_quarter,
            date_month
    )
    SELECT
        cb.tax_code,
        cb.customer_name,
        cb.contract_type,
        cb.product_category,
        cb.sign_date,
        cb.expire_date,
        cb.is_next_signed_within_90_days,
        cb.is_returning_customer,
        cb.first_customer_sign_date,
        cb.first_customer_product_sign_date,
        m.year,
        m.quarter,
        m.month,
        m.period_start,
        m.period_end,

        MAX(
            CASE
                WHEN cb.sign_date <= m.period_start
                AND cb.expire_date >= m.period_start
                THEN 1 ELSE 0
            END
        ) AS is_active_at_period_start,

        MAX(
            CASE
                WHEN cb.sign_date <= m.period_end
                AND cb.expire_date >= m.period_end
                THEN 1 ELSE 0
            END
        ) AS is_active_at_period_end,

        MAX(
            CASE
                WHEN cb.expire_date BETWEEN m.period_start AND m.period_end
                THEN 1 ELSE 0
            END
        ) AS is_expiring_customer,

        MAX(
            CASE
                WHEN cb.contract_type IN ('renew','renewal')
                AND cb.sign_date BETWEEN m.period_start AND m.period_end
                THEN 1 ELSE 0
            END
        ) AS is_renewal,

        MAX(
            CASE
                WHEN cb.contract_type IN ('upsales')
                AND cb.sign_date BETWEEN m.period_start AND m.period_end
                THEN 1 ELSE 0
            END
        ) AS is_upsell,
        
        MAX(
            CASE
                WHEN cb.first_customer_sign_date BETWEEN m.period_start AND m.period_end
                THEN 1 ELSE 0
            END
        ) AS is_new_customer_vcs,
        
        MAX(
            CASE
                WHEN cb.first_customer_product_sign_date BETWEEN m.period_start AND m.period_end
                THEN 1 ELSE 0
            END
        ) AS is_new_customer_product,
                                        
        CASE
            WHEN sign_date <= m.period_end
            AND expire_date BETWEEN m.period_end
                                AND date_add(m.period_end, 90)
            THEN 1 ELSE 0
        END AS is_expiring_90d,
                                        
        CASE
            WHEN expire_date < m.period_end
            AND datediff(m.period_end, expire_date) <= 90
            THEN 1 ELSE 0
        END AS is_grace_period                                        
        
    FROM customer_product_lifecycle_base_src cb
    JOIN dim_month m
        ON cb.sign_date <= m.period_end
    AND cb.expire_date >= m.period_start
    GROUP BY
        cb.tax_code,
        cb.customer_name,
        cb.contract_type,
        cb.product_category,
        cb.sign_date,
        cb.expire_date,
        cb.is_next_signed_within_90_days,
        cb.is_returning_customer,
        cb.first_customer_sign_date,
        cb.first_customer_product_sign_date,
        m.year,
        m.quarter,
        m.month,
        m.period_start,
        m.period_end
''')
customer_snapshot_month_src.createOrReplaceTempView("customer_snapshot_month_src")

sql_query = """
SELECT

tax_code,
customer_name,
product_category,
contract_type,
sign_date,
expire_date,
first_customer_sign_date,
first_customer_product_sign_date,

year,
quarter,
month,
period_start,
period_end,

is_active_at_period_start,
is_active_at_period_end,
is_expiring_customer,

is_renewal,
is_upsell,
is_next_signed_within_90_days,
is_returning_customer,


is_new_customer_vcs,
is_new_customer_product,
is_expiring_90d,
is_grace_period,

CASE
    WHEN is_active_at_period_end = 1
         OR first_customer_product_sign_date BETWEEN period_start AND period_end
    THEN 1
    ELSE 0
END AS is_active

FROM customer_snapshot_month_src
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
#   .save(target_path)
  .option("path", target_path)
  .saveAsTable(target_table)
)
spark.catalog.refreshTable(target_table)
print(target_table)