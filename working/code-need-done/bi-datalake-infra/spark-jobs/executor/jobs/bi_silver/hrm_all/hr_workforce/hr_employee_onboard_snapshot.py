# %livy.pyspark

target_table = "bi_silver.hr_employee_onboard_snapshot"
target_path = "s3a://bi-silver/hr_employee_snapshot"

spark.sql("REFRESH TABLE hr_raw.hr_employee_onboard_logs")
spark.sql("REFRESH TABLE bi_silver.dim_date")

sql_query = """
WITH tmp_parsed_dates AS (
    SELECT
        o.*,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(o.snapshot_date, 'yyyy-MM-dd'),
                TO_TIMESTAMP(o.snapshot_date, 'dd/MM/yyyy'),
                TO_TIMESTAMP(o.snapshot_date, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(o.snapshot_date_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(o.snapshot_date_ts AS BIGINT))
            )
        ) AS snapshot_date_parsed,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(o.hire_date_vcs, 'yyyy-MM-dd'),
                TO_TIMESTAMP(o.hire_date_vcs, 'dd/MM/yyyy'),
                TO_TIMESTAMP(o.hire_date_vcs, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(o.hire_date_vcs_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(o.hire_date_vcs_ts AS BIGINT))
            )
        ) AS hire_date_vcs_parsed,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(o.termination_date, 'yyyy-MM-dd'),
                TO_TIMESTAMP(o.termination_date, 'dd/MM/yyyy'),
                TO_TIMESTAMP(o.termination_date, 'yyyy-MM-dd HH:mm:ss'),
                FROM_UNIXTIME(CAST(o.termination_date_ts / 1000 AS BIGINT)),
                FROM_UNIXTIME(CAST(o.termination_date_ts AS BIGINT))
            )
        ) AS termination_date_parsed,

        TO_DATE(
            COALESCE(
                TO_TIMESTAMP(o.dob, 'yyyy-MM-dd'),
                TO_TIMESTAMP(o.dob, 'dd/MM/yyyy'),
                TO_TIMESTAMP(o.dob, 'yyyy-MM-dd HH:mm:ss')
            )
        ) AS date_of_birth_parsed

    FROM hr_raw.hr_employee_onboard_logs o
),

tmp_enriched_dates AS (
    SELECT 
        src.*,
        sd.date_key AS snapshot_date_key,
        sd.date_value AS snapshot_date_val,
        sd.date_year AS snapshot_year_val,
        sd.date_month AS snapshot_month_val,
        sd.date_month_start AS snapshot_month_start_val,
        sd.date_year_month AS snapshot_year_month_val
    FROM tmp_parsed_dates src
    LEFT JOIN bi_silver.dim_date sd
        ON src.snapshot_date_parsed = sd.date_value
),

tmp_base_metrics AS (
    SELECT
        *,

        CASE
            WHEN termination_date_parsed IS NULL OR termination_date_parsed > snapshot_date_val THEN 1
            ELSE 0
        END AS is_active_calculated,

        CASE
            WHEN hire_date_vcs_parsed IS NULL OR snapshot_date_val IS NULL THEN NULL
            ELSE FLOOR(MONTHS_BETWEEN(snapshot_date_val, hire_date_vcs_parsed))
        END AS tenure_months_calculated,

        CASE
            WHEN date_of_birth_parsed IS NULL OR snapshot_date_val IS NULL THEN NULL
            WHEN date_of_birth_parsed > snapshot_date_val THEN NULL
            ELSE FLOOR(MONTHS_BETWEEN(snapshot_date_val, date_of_birth_parsed) / 12)
        END AS age_calculated

    FROM tmp_enriched_dates
)

SELECT
    CAST(employee_id AS STRING) AS employee_code,
    employee_name AS full_name,
    LOWER(business_email) AS business_email,

    CASE
        WHEN business_email IS NULL OR TRIM(business_email) = '' THEN NULL
        WHEN INSTR(business_email, '@') = 0 THEN LOWER(TRIM(business_email))
        ELSE SPLIT(LOWER(TRIM(business_email)), '@')[0]
    END AS email_nametag,

    date_of_birth_parsed AS date_of_birth,
    mobile_phone,
    snapshot_date_val AS snapshot_date,
    snapshot_year_val AS snapshot_year,
    snapshot_month_val AS snapshot_month,
    snapshot_month_start_val AS snapshot_month_start,
    snapshot_year_month_val AS snapshot_year_month,

    status,
    hire_status,
    employee_group AS employee_type,
    contract_company_type AS contract_type,

    CASE
        WHEN NULLIF(TRIM(employee_group), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'CTV' THEN 'CTV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) IN ('IS', 'OS') THEN 'IS/OS'
        ELSE COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group))
    END AS resource_type,

    branch_name AS branch,
    division_name AS division_n,
    n1_group AS department_n_1,
    n2_group AS department_n_2,
    n3_group AS department_n_3,

    job_title AS role_base,
    employee_level AS job_level,
    management_level,

    COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) AS position_level,

    CASE
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'N' THEN 1
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'N-1' THEN 2
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'N-2' THEN 3
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Expert' THEN 4
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Specialist' THEN 5
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Senior' THEN 6
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Experienced' THEN 7
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Junior' THEN 8
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Entry' THEN 9
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'CTV' THEN 10
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'Fresher' THEN 11
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'SV' THEN 12
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'IS' THEN 13
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'OS' THEN 14
    END AS hierarchy_level,

    TRIM(regional_staff) AS competency_zone,

    hire_date_vcs_parsed AS hire_date_vcs,
    termination_date_parsed AS termination_date,

    gender,

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

    tenure,

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

    is_active_calculated AS is_active,

    CASE WHEN LOWER(TRIM(is_new_hire)) IN ('x', '1', 'true', 'yes', 'y') THEN 1 ELSE 0 END AS is_new_hire,
    CASE WHEN LOWER(TRIM(voluntary_exit)) IN ('x', '1', 'true', 'yes', 'y') THEN 1 ELSE 0 END AS is_voluntary_exit,
    CASE WHEN LOWER(TRIM(company_termination)) IN ('x', '1', 'true', 'yes', 'y') THEN 1 ELSE 0 END AS is_company_termination,

    performance_ranking,
    potential_ranking,
    talent_group,

    crawled_at_ts,
    snapshot_date_ts,
    filename

FROM tmp_base_metrics
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .partitionBy("snapshot_year") \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("Done:", target_table)