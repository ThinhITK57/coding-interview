%livy.pyspark

from pyspark.sql.functions import col, lit

# =========================
# 1. Config
# =========================
src_hr_table = "hr_raw.hr_employee_onboard_logs"
tgt_hr_table = "bi_silver.hr_employee_onboard"
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_employee_onboard"

spark.sql("REFRESH TABLE " + src_hr_table)

df_hr = spark.table(src_hr_table)
df_hr.createOrReplaceTempView("hr_raw_view")


# =========================
# 2. Add missing columns only
# =========================
new_columns = {
    "resource_type": "STRING",
    "position_level": "STRING",
    "hierarchy_level": "INT"
}

target_cols_now = spark.table(tgt_hr_table).columns
target_cols_now_lower = [c.lower() for c in target_cols_now]

cols_to_add = []
for c, t in new_columns.items():
    if c.lower() not in target_cols_now_lower:
        cols_to_add.append("{0} {1}".format(c, t))

if len(cols_to_add) > 0:
    spark.sql("""
        ALTER TABLE {0} ADD COLUMNS (
            {1}
        )
    """.format(tgt_hr_table, ",\n            ".join(cols_to_add)))
    print("Added columns: " + ", ".join(cols_to_add))
else:
    print("No columns need to be added")

spark.sql("REFRESH TABLE " + tgt_hr_table)


# =========================
# 3. Create tmp transformed view
#    Lay ban ghi moi nhat theo employee_id
# =========================
tmp_sql = """
WITH latest_raw AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(employee_id AS STRING)
                ORDER BY CAST(snapshot_date_ts AS BIGINT) DESC
            ) AS rn
        FROM hr_raw_view
        WHERE employee_id IS NOT NULL
          AND TRIM(CAST(employee_id AS STRING)) <> ''
    ) t
    WHERE rn = 1
),

base AS (
    SELECT
        *,
        COALESCE(
            NULLIF(NULLIF(TRIM(management_level), ''), '0'),
            employee_level
        ) AS effective_position_level
    FROM latest_raw
)

SELECT 
    CAST(employee_id AS STRING) AS employee_code,
    employee_name AS full_name,
    status AS current_status,
    hire_status AS recruitment_status,
    employee_group AS employee_type,

    CASE
        WHEN NULLIF(TRIM(employee_group), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN effective_position_level = 'CTV' THEN 'CTV'
        WHEN effective_position_level IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN effective_position_level IN ('IS', 'OS') THEN 'IS/OS'
    END AS resource_type,

    contract_company_type AS contract_company_independent,
    branch_name AS branch,
    TRIM(division_name) AS division_n,
    TRIM(n1_group) AS department_n_1,
    TRIM(n2_group) AS department_n_2,
    TRIM(n3_group) AS department_n_3,
    job_title AS role_base,
    employee_level AS job_level,
    regional_staff AS technical_grade_band,

    CASE
        WHEN regional_staff LIKE '%Vùng 1%' THEN 1
        WHEN regional_staff LIKE '%Vùng 2%' THEN 2
        WHEN regional_staff LIKE '%Vùng 3%' THEN 3
        ELSE 0
    END AS technical_grade_level,

    management_level,
    effective_position_level AS position_level,

    CASE
        WHEN effective_position_level = 'N' THEN 1
        WHEN effective_position_level = 'N-1' THEN 2
        WHEN effective_position_level = 'N-2' THEN 3
        WHEN effective_position_level = 'Expert' THEN 4
        WHEN effective_position_level = 'Specialist' THEN 5
        WHEN effective_position_level = 'Senior' THEN 6
        WHEN effective_position_level = 'Experienced' THEN 7
        WHEN effective_position_level = 'Junior' THEN 8
        WHEN effective_position_level = 'Entry' THEN 9
        WHEN effective_position_level = 'CTV' THEN 10
        WHEN effective_position_level = 'Fresher' THEN 11
        WHEN effective_position_level = 'SV' THEN 12
        WHEN effective_position_level = 'IS' THEN 13
        WHEN effective_position_level = 'OS' THEN 14
    END AS hierarchy_level,

    TO_TIMESTAMP(TO_DATE(hire_date_vcs, 'yyyy-MM-dd')) AS hire_date,
    TO_TIMESTAMP(TO_DATE(hire_date_viettel, 'yyyy-MM-dd')) AS hire_date_viettel,
    TO_TIMESTAMP(TO_DATE(termination_date, 'yyyy-MM-dd')) AS termination_date,

    LOWER(business_email) AS business_email,
    SPLIT(LOWER(TRIM(business_email)), '@')[0] AS email_nametag,

    TO_TIMESTAMP(TO_DATE(dob, 'yyyy-MM-dd')) AS date_of_birth,
    gender,

    CAST(pr_q1_2024 AS DOUBLE) AS pr_q1_2024,
    CAST(pr_q2_2024 AS DOUBLE) AS pr_q2_2024,
    CAST(pr_q3_2024 AS DOUBLE) AS pr_q3_2024,
    CAST(pr_q4_2024 AS DOUBLE) AS pr_q4_2024,

    CAST(pr_q1_2025 AS DOUBLE) AS pr_q1_2025,
    CAST(pr_q2_2025 AS DOUBLE) AS pr_q2_2025,
    CAST(pr_q3_2025 AS DOUBLE) AS pr_q3_2025,

    achievement_title_2024,
    talent_group,

    CASE
        WHEN talent_group IN ('Stars', 'High Performers', 'Disfuntional Geniuses') THEN 'HIGH'
        WHEN talent_group IN ('Core Players', 'High Potentials', 'Workhorses') THEN 'MEDIUM'
        ELSE 'LOW'
    END AS performance_level,

    CASE
        WHEN talent_group IN ('Stars', 'High Potentials') THEN 'HIGH'
        WHEN talent_group IN ('High Performers', 'Core Players', 'Up/Out Dilemmas') THEN 'MEDIUM'
        ELSE 'LOW'
    END AS potential_level,

    termination_status,
    termination_reason_1,
    termination_reason_2,
    termination_detail_reason,

    CAST(next_workplace AS DOUBLE) AS next_workplace,

    is_new_hire,
    CAST(age AS INT) AS age,

    CASE
        WHEN CAST(age AS INT) < 25 THEN '<25'
        WHEN CAST(age AS INT) BETWEEN 25 AND 29 THEN '25-29'
        WHEN CAST(age AS INT) BETWEEN 30 AND 34 THEN '30-34'
        WHEN CAST(age AS INT) BETWEEN 35 AND 39 THEN '35-39'
        ELSE '40+'
    END AS age_group,

    CASE WHEN LOWER(TRIM(is_core_employee)) = 'x' THEN 1 ELSE 0 END AS key_core_group,
    CASE WHEN LOWER(TRIM(`key`)) = 'x' THEN 1 ELSE 0 END AS is_key_employee,

    CASE
        WHEN LOWER(TRIM(`key`)) = 'x' AND LOWER(TRIM(hp)) = 'x' THEN 'CORE'
        WHEN LOWER(TRIM(`key`)) = 'x' THEN 'KEY'
        WHEN LOWER(TRIM(hp)) = 'x' THEN 'POTENTIAL'
        ELSE 'NON_CRITICAL'
    END AS employee_criticality_level

FROM base
"""

df_tmp = spark.sql(tmp_sql)
df_tmp.createOrReplaceTempView("tmp_hr_employee_onboard")

print("TMP columns:", len(df_tmp.columns))


# =========================
# 4. Align tmp theo schema target
# =========================
target_df = spark.table(tgt_hr_table)
target_schema = target_df.schema
target_cols = target_df.columns

tmp_cols_lower = [c.lower() for c in df_tmp.columns]

for field in target_schema.fields:
    if field.name.lower() not in tmp_cols_lower:
        df_tmp = df_tmp.withColumn(field.name, lit(None).cast(field.dataType))

df_final = df_tmp.select([col(c) for c in target_cols])
df_final.createOrReplaceTempView("tmp_hr_employee_onboard_final")

print("Target columns:", len(target_cols))
print("Final columns:", len(df_final.columns))


# =========================
# 5. Insert overwrite
# =========================
df_final.write.mode("overwrite").insertInto(tgt_hr_table)

spark.sql("REFRESH TABLE " + tgt_hr_table)

print("--- SUCCESS: Insert overwrite completed ---")
print(tgt_hr_table)