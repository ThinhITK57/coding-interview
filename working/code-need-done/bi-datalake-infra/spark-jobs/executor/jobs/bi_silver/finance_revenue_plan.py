# %livy.pyspark

# =========================================================
# FINANCE REVENUE PLAN
#
# OLD: bi_silver.biz_finance_plan
# NEW: bi_silver.finance_revenue_plan
# =========================================================

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_finance_plan")
# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_revenue_plan")


# =========================================================
# 0. CONFIG
# =========================================================

src_fin = "finance_raw.finance_plan"

tgt_fin_path = "s3a://bi-silver/finance_revenue_plan"
tgt_fin_table = "bi_silver.finance_revenue_plan"

spark.conf.set(
    "spark.sql.parquet.int96RebaseModeInRead",
    "CORRECTED"
)

spark.conf.set(
    "spark.sql.parquet.enableVectorizedReader",
    "false"
)

spark.catalog.clearCache()

spark.sql("REFRESH TABLE " + src_fin)


# =========================================================
# 1. LOAD SOURCE
# =========================================================

df_raw = spark.table(src_fin)

df_raw.createOrReplaceTempView("raw_fin")


# =========================================================
# 2. TRANSFORM
# =========================================================

query = """
WITH source_data AS (
    SELECT
        *,

        CAST(plan_date AS TIMESTAMP) AS ts_plan_date,

        TRIM(CAST(segment AS STRING)) AS segment_clean,

        TRIM(CAST(subgroup AS STRING)) AS subgroup_clean,

        CAST(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(CAST(plan_viettel_group AS STRING)),
                    ',',
                    ''
                ),
                ''
            )
            AS DECIMAL(18,2)
        ) AS plan_viettel_group_amount_clean,

        CAST(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(CAST(plan_must AS STRING)),
                    ',',
                    ''
                ),
                ''
            )
            AS DECIMAL(18,2)
        ) AS plan_must_amount_clean,

        CAST(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(CAST(plan_nice AS STRING)),
                    ',',
                    ''
                ),
                ''
            )
            AS DECIMAL(18,2)
        ) AS plan_nice_amount_clean

    FROM raw_fin
),

normalized_l1 AS (
    SELECT
        *,

        CASE
            WHEN segment_clean IS NULL
              OR segment_clean = ''
                THEN 'Khác'

            WHEN segment_clean IN (
                'Khác',
                'SI ngoài',
                'Ngoài nội địa',
                'DT khác',
                'DT ngoài trong nước',
                'DT SI'
            )
                THEN 'Khách hàng ngoài'

            WHEN segment_clean IN (
                'BQP',
                'DT BQP'
            )
                THEN 'Bộ Quốc phòng'

            WHEN segment_clean IN (
                'SI Nội bộ',
                'SI nội bộ',
                'Nội bộ',
                'DT nội bộ'
            )
                THEN 'Nội bộ'

            WHEN segment_clean IN (
                'Thị trường Viettel',
                'Quốc tế ngoài',
                'DT quốc tế'
            )
                THEN 'International'

            ELSE segment_clean
        END AS normalized_segment_l1

    FROM source_data
),

normalized_l2 AS (
    SELECT
        *,

        CASE
            -- =================================================
            -- Mapping trực tiếp theo segment
            -- =================================================

            WHEN segment_clean = 'DT SI'
                THEN 'Khách hàng SI ngoài'

            WHEN segment_clean = 'SI ngoài'
                THEN 'Khách hàng SI ngoài'

            WHEN segment_clean IN (
                'SI Nội bộ',
                'SI nội bộ'
            )
                THEN 'Khách hàng SI nội bộ'

            WHEN segment_clean = 'Thị trường Viettel'
                THEN 'Direct / Local Channel'

            WHEN segment_clean = 'Quốc tế ngoài'
                THEN 'Viettel Global Partner'


            -- =================================================
            -- Nhóm Nội bộ
            -- =================================================

            WHEN normalized_segment_l1 = 'Nội bộ'
             AND LOWER(subgroup_clean) = 'si nội bộ'
                THEN 'Khách hàng SI nội bộ'

            WHEN normalized_segment_l1 = 'Nội bộ'
             AND LOWER(subgroup_clean) = 'si ngoài'
                THEN 'Khách hàng SI nội bộ'

            WHEN normalized_segment_l1 = 'Nội bộ'
                THEN 'Khách hàng nội bộ'


            -- =================================================
            -- Nhóm Khách hàng ngoài
            -- =================================================

            WHEN normalized_segment_l1 = 'Khách hàng ngoài'
             AND LOWER(subgroup_clean) IN (
                    'si nội bộ',
                    'si ngoài'
                 )
                THEN 'Khách hàng SI ngoài'

            WHEN normalized_segment_l1 = 'Khách hàng ngoài'
                THEN 'Khách hàng ngoài'


            -- =================================================
            -- Nhóm International
            -- =================================================

            WHEN normalized_segment_l1 = 'International'
             AND subgroup_clean = 'Thị trường'
                THEN 'Direct / Local Channel'

            WHEN normalized_segment_l1 = 'International'
             AND subgroup_clean = 'Quốc tế'
                THEN 'Viettel Global Partner'

            WHEN normalized_segment_l1 = 'International'
             AND subgroup_clean = 'Direct / Local Channel'
                THEN 'Direct / Local Channel'

            WHEN normalized_segment_l1 = 'International'
             AND subgroup_clean = 'Viettel Global Partner'
                THEN 'Viettel Global Partner'


            -- =================================================
            -- Mapping subgroup còn lại
            -- =================================================

            WHEN subgroup_clean = 'Thị trường'
                THEN 'Direct / Local Channel'

            WHEN subgroup_clean = 'Quốc tế'
                THEN 'Viettel Global Partner'

            WHEN LOWER(subgroup_clean) = 'si nội bộ'
                THEN 'Khách hàng SI nội bộ'

            WHEN LOWER(subgroup_clean) = 'si ngoài'
                THEN 'Khách hàng SI ngoài'


            -- =================================================
            -- Fallback
            -- =================================================

            WHEN subgroup_clean IS NULL
              OR subgroup_clean = ''
                THEN NULL

            ELSE subgroup_clean
        END AS normalized_segment_l2

    FROM normalized_l1
)

SELECT
    TO_TIMESTAMP(
        TO_DATE(ts_plan_date)
    ) AS plan_date,

    YEAR(ts_plan_date) AS plan_year,

    MONTH(ts_plan_date) AS plan_month,

    plan_viettel_group_amount_clean
        AS plan_viettel_group_amount,

    plan_must_amount_clean
        AS plan_must_amount,

    plan_nice_amount_clean
        AS plan_nice_amount,

    segment_clean AS segment_l1_alias,

    'VND' AS currency_code,

    normalized_segment_l1 AS segment_l1,

    normalized_segment_l1 AS customer_segment_l1,

    normalized_segment_l2 AS segment_l2,

    normalized_segment_l2 AS customer_segment_l2

FROM normalized_l2
"""


df_final = spark.sql(query)


# =========================================================
# 3. WRITE TARGET
# =========================================================

df_final.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_fin_path) \
    .saveAsTable(tgt_fin_table)


# =========================================================
# 4. REFRESH TARGET
# =========================================================

spark.catalog.clearCache()

spark.sql("REFRESH TABLE " + tgt_fin_table)

print("Write completed: " + tgt_fin_table)