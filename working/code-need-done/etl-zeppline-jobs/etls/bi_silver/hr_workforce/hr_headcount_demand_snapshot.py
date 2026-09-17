%livy.pyspark

target_table = "bi_silver.hr_headcount_demand_snapshot"
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_headcount_demand_snapshot"

spark.sql("REFRESH TABLE hr_raw.hr_employee_headcount_logs")
spark.sql("REFRESH TABLE bi_silver.dim_date")

sql_query = """
WITH src AS (
    SELECT
        h.*,

        TRIM(h.headcount_plan) AS headcount_plan_clean,

        REGEXP_EXTRACT(TRIM(h.headcount_plan), '^(\\\\S+)', 1) AS plan_type,

        CAST(
            REGEXP_EXTRACT(TRIM(h.headcount_plan), '(20[0-9]{2})', 1)
            AS INT
        ) AS plan_year,

        CAST(
            CONCAT(
                REGEXP_EXTRACT(TRIM(h.headcount_plan), '(20[0-9]{2})', 1),
                '0101'
            ) AS INT
        ) AS plan_date_key,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(h.snapshot_date, 'yyyy-MM-dd'),
                TO_TIMESTAMP(h.snapshot_date, 'dd/MM/yyyy'),
                TO_TIMESTAMP(h.snapshot_date, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(h.snapshot_date_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(h.snapshot_date_ts AS BIGINT))
            )
        ) AS snapshot_date_parsed

    FROM hr_raw.hr_employee_headcount_logs h
)

SELECT
    src.plan_date_key,
    pd.date_value AS plan_date,
    src.plan_year,
    src.plan_type,
    src.headcount_plan_clean AS headcount_plan,

    sd.date_key AS snapshot_date_key,
    sd.date_value AS snapshot_date,
    sd.date_year AS snapshot_year,
    sd.date_month AS snapshot_month,
    sd.date_month_start AS snapshot_month_start,
    sd.date_year_month AS snapshot_year_month,

    src.headcount_status,
    src.position_status,

    src.division_n,
    src.unit_n_1,
    src.department_n_2,
    src.department_n_3,
    src.branch,
    src.region as competency_zone,

    src.role_base,
    src.specialization,
    src.required_level,
    src.management_level,

    COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) AS position_level,

    CASE
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'N' THEN 1
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'N-1' THEN 2
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'N-2' THEN 3
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Expert' THEN 4
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Specialist' THEN 5
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Senior' THEN 6
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Experienced' THEN 7
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Junior' THEN 8
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Entry' THEN 9
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'CTV' THEN 10
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'Fresher' THEN 11
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'SV' THEN 12
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'IS' THEN 13
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'OS' THEN 14
        ELSE 99
    END AS hierarchy_level,

    src.job_framework_position,
    src.job_framework_position_2,
    src.service_group,
    src.service_group_new,

    src.employee_code,
    src.full_name,
    LOWER(src.business_email) AS business_email,
    src.employee_object AS employee_type,
    src.contract_type,

    CASE
        WHEN NULLIF(TRIM(src.employee_object), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) = 'CTV' THEN 'CTV'
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN COALESCE(NULLIF(TRIM(src.management_level), ''), src.required_level) IN ('IS', 'OS') THEN 'IS/OS'
        ELSE 'Official'
    END AS resource_type,

    CASE WHEN src.employee_code IS NOT NULL AND TRIM(src.employee_code) <> '' THEN 1 ELSE 0 END AS is_filled,
    CASE WHEN src.employee_code IS NULL OR TRIM(src.employee_code) = '' THEN 1 ELSE 0 END AS is_vacant,

    CASE WHEN LOWER(TRIM(src.is_ai_group)) = 'x' THEN 1 ELSE 0 END AS is_ai_group,
    CASE WHEN LOWER(TRIM(src.is_ai_org)) = 'x' THEN 1 ELSE 0 END AS is_ai_org,
    CASE WHEN LOWER(TRIM(src.is_philippines_japan_market)) = 'x' THEN 1 ELSE 0 END AS is_philippines_japan_market,
    CASE WHEN LOWER(TRIM(src.is_domestic_business)) = 'x' THEN 1 ELSE 0 END AS is_domestic_business,
    CASE WHEN LOWER(TRIM(src.is_international_business)) = 'x' THEN 1 ELSE 0 END AS is_international_business,
    CASE WHEN LOWER(TRIM(src.is_rnd)) = 'x' THEN 1 ELSE 0 END AS is_rnd,
    CASE WHEN LOWER(TRIM(src.is_indirect_group)) = 'x' THEN 1 ELSE 0 END AS is_indirect_group,
    CASE WHEN LOWER(TRIM(src.is_business_support)) = 'x' THEN 1 ELSE 0 END AS is_business_support,

    src.crawled_at_ts,
    src.snapshot_date_ts,
    src.filename,
    src.sheet_name

FROM src
LEFT JOIN bi_silver.dim_date pd
    ON src.plan_date_key = pd.date_key
LEFT JOIN bi_silver.dim_date sd
    ON src.snapshot_date_parsed = sd.date_value
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .partitionBy("plan_year", "snapshot_year") \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("Done:", target_table)