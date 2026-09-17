# %livy.pyspark

sql_query = """
WITH last_cm_kpi AS (
    SELECT *
    FROM (
        SELECT
            ss.*,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at_ts DESC, id DESC
            ) AS rn
        FROM crm_raw.cm_kpi ss
    ) t
    WHERE rn = 1
)

SELECT
    k.id,
    k.name,
    k.owner_id,

    -- ===== custom_field =====
    k.custom_field.cf_kpi_scope                   AS kpi_scope,
    k.custom_field.cf_currency                    AS currency_code,
    k.custom_field.cf_relate_kpi                  AS relate_kpi_id,
    k.custom_field.cf_sale_target                 AS sale_target,
    k.custom_field.cf_revenue_target              AS revenue_target,

    -- Q1
    k.custom_field.cf_sale_target_quarter_1        AS sale_target_q1,
    k.custom_field.cf_sale_target_month_1          AS sale_target_m1,
    k.custom_field.cf_sale_target_month_2          AS sale_target_m2,
    k.custom_field.cf_sale_target_month_3          AS sale_target_m3,
    k.custom_field.cf_revenue_target_quarter_1     AS revenue_target_q1,
    k.custom_field.cf_revenue_target_month_1       AS revenue_target_m1,
    k.custom_field.cf_revenue_target_month_2       AS revenue_target_m2,
    k.custom_field.cf_revenue_target_month_3       AS revenue_target_m3,

    -- Q2
    k.custom_field.cf_sale_target_quarter_2        AS sale_target_q2,
    k.custom_field.cf_sale_target_month_4          AS sale_target_m4,
    k.custom_field.cf_sale_target_month_5          AS sale_target_m5,
    k.custom_field.cf_sale_target_month_6          AS sale_target_m6,
    k.custom_field.cf_revenue_target_quarter_2     AS revenue_target_q2,
    k.custom_field.cf_revenue_target_month_4       AS revenue_target_m4,
    k.custom_field.cf_revenue_target_month_5       AS revenue_target_m5,
    k.custom_field.cf_revenue_target_month_6       AS revenue_target_m6,

    -- Q3
    k.custom_field.cf_sale_target_quarter_3        AS sale_target_q3,
    k.custom_field.cf_sale_target_month_7          AS sale_target_m7,
    k.custom_field.cf_sale_target_month_8          AS sale_target_m8,
    k.custom_field.cf_sale_target_month_9          AS sale_target_m9,
    k.custom_field.cf_revenue_target_quarter_3     AS revenue_target_q3,
    k.custom_field.cf_revenue_target_month_7       AS revenue_target_m7,
    k.custom_field.cf_revenue_target_month_8       AS revenue_target_m8,
    k.custom_field.cf_revenue_target_month_9       AS revenue_target_m9,

    -- Q4
    k.custom_field.cf_sale_target_quarter_4        AS sale_target_q4,
    k.custom_field.cf_sale_target_month_10         AS sale_target_m10,
    k.custom_field.cf_sale_target_month_11         AS sale_target_m11,
    k.custom_field.cf_sale_target_month_12         AS sale_target_m12,
    k.custom_field.cf_revenue_target_quarter_4     AS revenue_target_q4,
    k.custom_field.cf_revenue_target_month_10      AS revenue_target_m10,
    k.custom_field.cf_revenue_target_month_11      AS revenue_target_m11,
    k.custom_field.cf_revenue_target_month_12      AS revenue_target_m12,

    -- ===== metadata =====
--    k.created_at,
    k.creator_id
--    k.updated_at,
--    k.updater_id,
--    k.avatar,
--    k.recent_note,

    -- links
--    k.links.document_associations                  AS document_associations,
--    k.links.notes                                  AS notes,

--    k.record_type_id,
--    k.created_at_ts,
--    k.updated_at_ts

FROM last_cm_kpi k

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/kpis"
    ) \
    .saveAsTable("crm_silver.kpis")
