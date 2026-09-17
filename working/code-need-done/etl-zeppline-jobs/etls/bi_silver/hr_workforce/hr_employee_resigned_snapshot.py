%livy.pyspark

target_table = "bi_silver.hr_employee_resigned_snapshot"
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_employee_resigned_snapshot"

# 1. Đồng bộ Metadata từ Data Lake
spark.sql("REFRESH TABLE hr_raw.hr_employee_resigned_logs")
spark.sql("REFRESH TABLE bi_silver.dim_date")

sql_query = """
WITH tmp_parsed_dates AS (
    SELECT
        r.*,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(r.snapshot_date, 'yyyy-MM-dd'),
                TO_TIMESTAMP(r.snapshot_date, 'dd/MM/yyyy'),
                TO_TIMESTAMP(r.snapshot_date, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(r.snapshot_date_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(r.snapshot_date_ts AS BIGINT))
            )
        ) AS snapshot_date_parsed,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(r.resigned_date, 'yyyy-MM-dd'),
                TO_TIMESTAMP(r.resigned_date, 'dd/MM/yyyy'),
                TO_TIMESTAMP(r.resigned_date, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(r.resigned_date_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(r.resigned_date_ts AS BIGINT))
            )
        ) AS resigned_date_parsed,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(r.hire_date_vcs, 'yyyy-MM-dd'),
                TO_TIMESTAMP(r.hire_date_vcs, 'dd/MM/yyyy'),
                TO_TIMESTAMP(r.hire_date_vcs, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(r.hire_date_vcs_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(r.hire_date_vcs_ts AS BIGINT))
            )
        ) AS hire_date_vcs_parsed,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(r.date_of_birth, 'yyyy-MM-dd'),
                TO_TIMESTAMP(r.date_of_birth, 'dd/MM/yyyy'),
                TO_TIMESTAMP(r.date_of_birth, 'yyyy-MM-dd HH:mm:ss')
            )
        ) AS date_of_birth_parsed

    FROM hr_raw.hr_employee_resigned_logs r
),

tmp_enriched_dates AS (
    SELECT
        src.*,

        sd.date_key AS snapshot_date_key_val,
        sd.date_value AS snapshot_date_val,
        sd.date_year_month AS snapshot_year_month_val,

        rd.date_key AS resigned_date_key_val,
        rd.date_value AS resigned_date_val,
        rd.date_year AS resigned_year_val,
        rd.date_month AS resigned_month_val,
        rd.date_month_start AS resigned_month_start_val,
        rd.date_year_month AS resigned_year_month_val

    FROM tmp_parsed_dates src
    LEFT JOIN bi_silver.dim_date sd
        ON src.snapshot_date_parsed = sd.date_value
    LEFT JOIN bi_silver.dim_date rd
        ON src.resigned_date_parsed = rd.date_value
),

tmp_calculated_metrics AS (
    SELECT
        *,

        CASE
            WHEN hire_date_vcs_parsed IS NULL OR resigned_date_parsed IS NULL THEN NULL
            ELSE FLOOR(MONTHS_BETWEEN(resigned_date_parsed, hire_date_vcs_parsed))
        END AS tenure_months_calculated,

        CASE
            WHEN date_of_birth_parsed IS NULL OR snapshot_date_parsed IS NULL THEN NULL
            WHEN date_of_birth_parsed > snapshot_date_parsed THEN NULL
            ELSE FLOOR(MONTHS_BETWEEN(snapshot_date_parsed, date_of_birth_parsed) / 12)
        END AS age_calculated

    FROM tmp_enriched_dates
)

SELECT
    employee_code,
    full_name,
    LOWER(business_email) AS business_email,

    CASE
        WHEN business_email IS NULL OR TRIM(business_email) = '' THEN NULL
        WHEN INSTR(business_email, '@') = 0 THEN LOWER(TRIM(business_email))
        ELSE SPLIT(LOWER(TRIM(business_email)), '@')[0]
    END AS email_nametag,

    date_of_birth_parsed AS date_of_birth,
    mobile_phone,
    talent_9box_group AS talent_group,
    gender,

    snapshot_date_val AS snapshot_date,
    snapshot_year_month_val AS snapshot_year_month,

    resigned_date_key_val AS resigned_date_key,
    resigned_date_val AS resigned_date,
    resigned_year_val AS resigned_year,
    resigned_month_val AS resigned_month,
    resigned_month_start_val AS resigned_month_start,
    resigned_year_month_val AS resigned_year_month,

    current_status,
    new_hire_status,
    employee_object AS employee_type,
    contract_type,

    CASE
        WHEN NULLIF(TRIM(employee_object), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'CTV' THEN 'CTV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) IN ('IS', 'OS') THEN 'IS/OS'
    END AS resource_type,

    branch,
    division_n,
    unit_n_1 AS department_n_1,
    department_n_2,
    department_n_3,

    role_base,
    level AS job_level,
    management_level,

    CASE
        WHEN management_level IS NOT NULL AND TRIM(management_level) <> ''
        THEN management_level
        ELSE level
    END AS position_level,

    CASE
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'N' THEN 1
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'N-1' THEN 2
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'N-2' THEN 3
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Expert' THEN 4
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Specialist' THEN 5
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Senior' THEN 6
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Experienced' THEN 7
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Junior' THEN 8
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Entry' THEN 9
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'CTV' THEN 10
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'Fresher' THEN 11
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'SV' THEN 12
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'IS' THEN 13
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'OS' THEN 14
    END AS hierarchy_level,

    region AS competency_zone,
    hire_date_vcs_parsed AS hire_date_vcs,

    resignation_status,
    resignation_reason_level_1,
    resignation_reason_level_2,
    resignation_reason_detail,
    next_workplace,

    seniority AS tenure,

    age_calculated AS age,

    CASE
        WHEN age_calculated IS NULL THEN 'Unknown'
        WHEN age_calculated < 25 THEN '<25'
        WHEN age_calculated < 30 THEN '25-29'
        WHEN age_calculated < 35 THEN '30-34'
        WHEN age_calculated < 40 THEN '35-39'
        WHEN age_calculated < 45 THEN '40-44'
        ELSE '45+'
    END AS age_group,

    tenure_months_calculated AS tenure_months,

    CASE
        WHEN tenure_months_calculated IS NULL THEN 'Unknown'
        WHEN tenure_months_calculated < 6 THEN '<6M'
        WHEN tenure_months_calculated < 12 THEN '6M-1Y'
        WHEN tenure_months_calculated < 36 THEN '1-3Y'
        WHEN tenure_months_calculated < 60 THEN '3-5Y'
        WHEN tenure_months_calculated < 84 THEN '5-7Y'
        WHEN tenure_months_calculated < 120 THEN '7-10Y'
        ELSE '>10Y'
    END AS tenure_group,

    CASE
        WHEN LOWER(TRIM(resignation_status)) LIKE '%chủ động%'
          OR LOWER(TRIM(resignation_status)) LIKE '%chu dong%'
          OR LOWER(TRIM(resignation_reason_level_1)) LIKE '%chủ động%'
          OR LOWER(TRIM(resignation_reason_level_1)) LIKE '%chu dong%'
        THEN 1 ELSE 0
    END AS is_voluntary_exit,

    CASE
        WHEN LOWER(TRIM(resignation_status)) LIKE '%thải%'
          OR LOWER(TRIM(resignation_status)) LIKE '%thai%'
          OR LOWER(TRIM(resignation_reason_level_1)) LIKE '%thải%'
          OR LOWER(TRIM(resignation_reason_level_1)) LIKE '%thai%'
        THEN 1 ELSE 0
    END AS is_company_termination,

    crawled_at_ts,
    snapshot_date_ts,
    filename

FROM tmp_calculated_metrics
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .partitionBy("resigned_year") \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("Done:", target_table)