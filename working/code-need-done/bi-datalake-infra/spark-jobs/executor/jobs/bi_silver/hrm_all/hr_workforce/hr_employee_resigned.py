# %livy.pyspark
from pyspark.sql.functions import col, lit

# =========================
# 1. Config
# =========================
src_resigned_table = "hr_raw.hr_employee_resigned_logs"
tgt_resigned_table = "bi_silver.hr_employee_resigned"
target_path = "s3a://bi-silver/hr_employee_resigned"

spark.sql("REFRESH TABLE " + src_resigned_table)

df_hr_out = spark.table(src_resigned_table)
df_hr_out.createOrReplaceTempView("hr_out_view")

# =========================
# 2. Ensure target columns
# =========================
expected_columns = {
    "resource_type": "STRING",
    "position_level": "STRING",
    "hierarchy_level": "INT"
}

target_cols_now = spark.table(tgt_resigned_table).columns
target_cols_now_lower = [c.lower() for c in target_cols_now]

cols_to_add = []
for c, t in expected_columns.items():
    if c.lower() not in target_cols_now_lower:
        cols_to_add.append("{0} {1}".format(c, t))

if len(cols_to_add) > 0:
    spark.sql("""
        ALTER TABLE {0} ADD COLUMNS (
            {1}
        )
    """.format(tgt_resigned_table, ",\n            ".join(cols_to_add)))

    print("Added columns: " + ", ".join(cols_to_add))
else:
    print("No columns need to be added")

spark.sql("REFRESH TABLE " + tgt_resigned_table)

# =========================
# 3. Transform tmp
# =========================
tmp_sql = """
WITH latest_raw AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(employee_code AS STRING)
                ORDER BY CAST(snapshot_date_ts AS BIGINT) DESC
            ) AS rn
        FROM hr_out_view
        WHERE employee_code IS NOT NULL
          AND TRIM(CAST(employee_code AS STRING)) <> ''
    ) t
    WHERE rn = 1
),

base AS (
    SELECT
        *,
        COALESCE(
            NULLIF(NULLIF(TRIM(management_level), ''), '0'),
            level
        ) AS effective_position_level
    FROM latest_raw
)

SELECT 
    CAST(employee_code AS STRING) AS employee_code,
    full_name,
    current_status,

    LOWER(TRIM(business_email)) AS business_email,
    SPLIT(LOWER(TRIM(business_email)), '@')[0] AS email_nametag,

    new_hire_status AS recruitment_status,
    employee_object AS employee_type,

    CASE
        WHEN NULLIF(TRIM(employee_object), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN effective_position_level = 'CTV' THEN 'CTV'
        WHEN effective_position_level IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN effective_position_level IN ('IS', 'OS') THEN 'IS/OS'
    END AS resource_type,

    contract_type AS contract_company_independent,

    branch,
    division_n AS division,
    unit_n_1 AS unit_level_1,
    unit_n_1 AS business_unit_level_1,
    department_n_2 AS department_level_2,
    department_n_3,

    role_base,
    level AS employee_level,
    region AS competency_zone,
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

    TO_TIMESTAMP(TO_DATE(hire_date_viettel)) AS hire_date,
    TO_TIMESTAMP(TO_DATE(hire_date_vcs)) AS hire_date_vcs,
    TO_TIMESTAMP(TO_DATE(resigned_date)) AS termination_date,
    TO_TIMESTAMP(TO_DATE(date_of_birth)) AS date_of_birth,

    gender,
    seniority AS seniority_group,

    CAST(pr_q1_2024 AS DOUBLE) AS pr_q1_2024,
    CAST(pr_q2_2024 AS DOUBLE) AS pr_q2_2024,
    CAST(pr_q3_2024 AS DOUBLE) AS pr_q3_2024,
    CAST(pr_q4_2024 AS DOUBLE) AS pr_q4_2024,

    CAST(pr_q1_2025 AS DOUBLE) AS pr_q1_2025,
    CAST(pr_q2_2025 AS DOUBLE) AS pr_q2_2025,

    award_2024 AS achievement_2024,
    talent_9box_group,

    CASE
        WHEN talent_9box_group IN ('Stars', 'High Performers', 'Disfuntional Geniuses') THEN 'HIGH'
        WHEN talent_9box_group IN ('Core Players', 'High Potentials', 'Workhorses') THEN 'MEDIUM'
        ELSE 'LOW'
    END AS performance_level,

    CASE
        WHEN talent_9box_group IN ('Stars', 'High Potentials') THEN 'HIGH'
        WHEN talent_9box_group IN ('High Performers', 'Core Players', 'Up/Out Dilemmas') THEN 'MEDIUM'
        ELSE 'LOW'
    END AS potential_level,

    resignation_status AS leave_status,
    resignation_reason_level_1,
    resignation_reason_level_2,
    resignation_reason_detail,

    CAST(next_workplace AS DOUBLE) AS next_workplace_score,

    CASE
        WHEN current_status = 'Active' THEN 'ACTIVE'
        WHEN current_status = 'Đang xin nghỉ' THEN 'PENDING_RESIGN'
        WHEN current_status LIKE 'Nghỉ việc%' THEN 'TERMINATED'
        WHEN current_status = 'Deactive' THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END AS employment_status,

    CASE
        WHEN current_status = 'Nghỉ việc (out chủ động)' THEN 'RESIGN_VOLUNTARY'
        WHEN current_status = 'Nghỉ việc (VCS cho nghỉ)' THEN 'RESIGN_INVOLUNTARY'
        WHEN current_status = 'Nghỉ việc (Fresher/SV/CTV)' THEN 'INTERN_END'
        ELSE NULL
    END AS termination_reason,

    CASE WHEN LOWER(TRIM(key_flag)) = 'x' THEN 1 ELSE 0 END AS is_key_employee,
    CASE WHEN LOWER(TRIM(hp_flag)) = 'x' THEN 1 ELSE 0 END AS is_high_potential

FROM base
"""

df_tmp = spark.sql(tmp_sql)
df_tmp.createOrReplaceTempView("tmp_hr_employee_resigned")

print("TMP columns:", len(df_tmp.columns))

# =========================
# 4. Align tmp theo schema target
# =========================
target_df = spark.table(tgt_resigned_table)
target_schema = target_df.schema
target_cols = target_df.columns

tmp_cols_lower = [c.lower() for c in df_tmp.columns]

for field in target_schema.fields:
    if field.name.lower() not in tmp_cols_lower:
        df_tmp = df_tmp.withColumn(field.name, lit(None).cast(field.dataType))

df_final = df_tmp.select([col(c) for c in target_cols])
df_final.createOrReplaceTempView("tmp_hr_employee_resigned_final")

# =========================
# 5. Insert overwrite
# =========================
df_final.write.mode("overwrite").insertInto(tgt_resigned_table)

spark.sql("REFRESH TABLE " + tgt_resigned_table)

num_cols = len(spark.table(tgt_resigned_table).columns)

print("--- SUCCESS: Insert overwrite completed ---")
print(tgt_resigned_table)