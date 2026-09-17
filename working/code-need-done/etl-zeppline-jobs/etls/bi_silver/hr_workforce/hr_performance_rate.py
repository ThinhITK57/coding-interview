%livy.pyspark

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# 1. CONFIG & REFRESH SOURCE
# ==============================================================================

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.catalog.clearCache()

src_tables = [
    "hr_raw.hr_employee_onboard_logs",
    "hr_raw.hr_employee_resigned_logs"
]

for src_table in src_tables:
    try:
        if spark.catalog.tableExists(src_table):
            try:
                spark.catalog.uncacheTable(src_table)
            except Exception as e:
                print("[WARNING] Khong the uncache {}: {}".format(src_table, str(e)))

            try:
                spark.catalog.refreshTable(src_table)
                print("Da refresh metadata cho {}".format(src_table))
            except Exception as e:
                print("[WARNING] Khong the refresh {}: {}".format(src_table, str(e)))
        else:
            print("[WARNING] Bang {} khong ton tai".format(src_table))
    except Exception as e:
        print("[WARNING] Khong the kiem tra bang {}: {}".format(src_table, str(e)))


# ==============================================================================
# 2. DEFINE TARGET
# ==============================================================================

tgt_table = "bi_silver.hr_performance_rate"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_performance_rate"



# ==============================================================================
# 3. TMP ONBOARD
# ==============================================================================

df_onboard = spark.sql("""
SELECT 
    CAST(employee_id AS STRING) AS employee_code,
    CAST(employee_name AS STRING) AS employee_name,

    CASE 
        WHEN business_email IS NOT NULL AND CAST(business_email AS STRING) LIKE '%@%' 
        THEN LOWER(SPLIT(CAST(business_email AS STRING), '@')[0])
        ELSE NULL
    END AS username,

    LOWER(CAST(business_email AS STRING)) AS business_email,
    CAST(gender AS STRING) AS gender,
    CAST(age_group AS STRING) AS age_group,
    CAST(branch_name AS STRING) AS branch,
    CAST(contract_company_type AS STRING) AS contract_type,
    CASE
        WHEN NULLIF(TRIM(employee_group), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) = 'CTV' THEN 'CTV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) IN ('IS', 'OS') THEN 'IS/OS'
        ELSE COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group))
    END AS resource_type,
    CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE) AS evaluation_date, 
    CAST(hire_date_vcs AS TIMESTAMP) AS hire_date_vcs,
    CAST(termination_date AS TIMESTAMP) AS termination_date,
    CAST(job_title AS STRING) AS role_base,
    CAST(tenure AS STRING) AS tenure,
    CASE
        WHEN hire_date_vcs IS NULL THEN NULL
        ELSE FLOOR(MONTHS_BETWEEN(
            CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE),
            CAST(hire_date_vcs AS DATE)
        ))
    END AS tenure_months,

    CASE
        WHEN hire_date_vcs IS NULL THEN 'Unknown'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(hire_date_vcs AS DATE))) < 6 THEN '<6M'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(hire_date_vcs AS DATE))) < 12 THEN '6M-1Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(hire_date_vcs AS DATE))) < 36 THEN '1-3Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(hire_date_vcs AS DATE))) < 60 THEN '3-5Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(hire_date_vcs AS DATE))) < 84 THEN '5-7Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(hire_date_vcs AS DATE))) < 120 THEN '7-10Y'
        ELSE '>10Y'
    END AS tenure_group,
    TRIM(division_name) AS division_n,
    TRIM(n1_group) AS department_n_1,
    TRIM(n2_group) AS department_n_2,
    TRIM(n3_group) AS department_n_3,
    CAST(employee_level AS STRING) AS job_level,
    COALESCE(NULLIF(TRIM(management_level), ''), TRIM(employee_level), TRIM(employee_group)) AS position_level,

    TRIM(regional_staff) AS competency_zone,
    CAST(raw_score AS INT) AS score,
    CAST(raw_ki AS STRING) AS ki,
    CAST(crawled_at_ts AS TIMESTAMP) AS crawled_at_ts,

    2 AS source_priority,
    CAST(crawled_at_ts AS TIMESTAMP) AS crawled_at_sort

FROM hr_raw.hr_employee_onboard_logs
LATERAL VIEW stack(12,
    '2024-03-31', pr_q1_2024, CAST(NULL AS STRING),
    '2024-06-30', pr_q2_2024, CAST(NULL AS STRING),
    '2024-09-30', pr_q3_2024, CAST(NULL AS STRING),
    '2024-12-31', pr_q4_2024, CAST(NULL AS STRING),
    '2025-03-31', pr_q1_2025, ki_q1_2025,
    '2025-06-30', pr_q2_2025, ki_q2_2025,
    '2025-09-30', pr_q3_2025, ki_q3_2025,
    '2025-12-31', pr_q4_2025, ki_q4_2025,
    '2026-03-31', pr_q1_2026, ki_q1_2026,
    '2026-06-30', pr_q2_2026, ki_q2_2026,
    '2026-09-30', pr_q3_2026, ki_q3_2026,
    '2026-12-31', pr_q4_2026, ki_q4_2026
) st AS eval_date_str, raw_score, raw_ki

WHERE raw_score IS NOT NULL 
  AND TRIM(CAST(raw_score AS STRING)) <> ''
  AND employee_id IS NOT NULL
  AND TRIM(CAST(employee_id AS STRING)) <> ''
""")

df_onboard.createOrReplaceTempView("tmp_hr_performance_onboard")


# ==============================================================================
# 4. TMP RESIGNED
# ==============================================================================

df_resigned = spark.sql("""
SELECT 
    CAST(employee_code AS STRING) AS employee_code,
    CAST(full_name AS STRING) AS employee_name,

    CASE 
        WHEN business_email IS NOT NULL AND CAST(business_email AS STRING) LIKE '%@%' 
        THEN LOWER(SPLIT(CAST(business_email AS STRING), '@')[0])
        ELSE NULL
    END AS username,

    LOWER(CAST(business_email AS STRING)) AS business_email,
    CAST(gender AS STRING) AS gender,
    CAST(age_group AS STRING) AS age_group,
    CAST(branch AS STRING) AS branch,
    CAST(contract_type AS STRING) AS contract_type,
    CASE
        WHEN NULLIF(TRIM(employee_object), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) = 'CTV' THEN 'CTV'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN COALESCE(NULLIF(TRIM(management_level), ''), level) IN ('IS', 'OS') THEN 'IS/OS'
        ELSE COALESCE(NULLIF(TRIM(management_level), ''), TRIM(level), TRIM(employee_object))
    END AS resource_type,
    CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE) AS evaluation_date, 

    CASE 
        WHEN hire_date_vcs_ts IS NOT NULL 
        THEN CAST(FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) AS TIMESTAMP)
        ELSE CAST(hire_date_vcs AS TIMESTAMP)
    END AS hire_date_vcs,

    CASE 
        WHEN resigned_date_ts IS NOT NULL 
        THEN CAST(FROM_UNIXTIME(CAST(resigned_date_ts AS BIGINT) / 1000) AS TIMESTAMP)
        ELSE CAST(resigned_date AS TIMESTAMP)
    END AS termination_date,

    CAST(job_framework_position AS STRING) AS role_base,
    CAST(seniority AS STRING) AS tenure,
    CASE
        WHEN hire_date_vcs_ts IS NULL AND hire_date_vcs IS NULL THEN NULL
        ELSE FLOOR(MONTHS_BETWEEN(
            CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE),
            CAST(
                CASE 
                    WHEN hire_date_vcs_ts IS NOT NULL 
                    THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000)
                    ELSE hire_date_vcs
                END AS DATE
            )
        ))
    END AS tenure_months,

    CASE
        WHEN hire_date_vcs_ts IS NULL AND hire_date_vcs IS NULL THEN 'Unknown'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(CASE WHEN hire_date_vcs_ts IS NOT NULL THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) ELSE hire_date_vcs END AS DATE))) < 6 THEN '<6M'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(CASE WHEN hire_date_vcs_ts IS NOT NULL THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) ELSE hire_date_vcs END AS DATE))) < 12 THEN '6M-1Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(CASE WHEN hire_date_vcs_ts IS NOT NULL THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) ELSE hire_date_vcs END AS DATE))) < 36 THEN '1-3Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(CASE WHEN hire_date_vcs_ts IS NOT NULL THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) ELSE hire_date_vcs END AS DATE))) < 60 THEN '3-5Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(CASE WHEN hire_date_vcs_ts IS NOT NULL THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) ELSE hire_date_vcs END AS DATE))) < 84 THEN '5-7Y'
        WHEN FLOOR(MONTHS_BETWEEN(CAST(TO_DATE(eval_date_str, 'yyyy-MM-dd') AS DATE), CAST(CASE WHEN hire_date_vcs_ts IS NOT NULL THEN FROM_UNIXTIME(CAST(hire_date_vcs_ts AS BIGINT) / 1000) ELSE hire_date_vcs END AS DATE))) < 120 THEN '7-10Y'
        ELSE '>10Y'
    END AS tenure_group,               
    TRIM(division_n) AS division_n,
    TRIM(unit_n_1) AS department_n_1,
    TRIM(department_n_2) AS department_n_2,
    TRIM(department_n_3) AS department_n_3,
    CAST(level AS STRING) AS job_level,

    CASE
        WHEN management_level IS NOT NULL AND TRIM(management_level) <> ''
        THEN management_level
        ELSE level
    END AS position_level,

    region AS competency_zone,

    CAST(raw_score AS INT) AS score,
    CAST(raw_ki AS STRING) AS ki,

    CASE 
        WHEN crawled_at_ts IS NOT NULL 
        THEN CAST(FROM_UNIXTIME(CAST(crawled_at_ts AS BIGINT) / 1000) AS TIMESTAMP)
        ELSE NULL
    END AS crawled_at_ts,

    1 AS source_priority,

    CASE 
        WHEN crawled_at_ts IS NOT NULL 
        THEN CAST(FROM_UNIXTIME(CAST(crawled_at_ts AS BIGINT) / 1000) AS TIMESTAMP)
        ELSE NULL
    END AS crawled_at_sort

FROM hr_raw.hr_employee_resigned_logs
LATERAL VIEW stack(12,
    '2024-03-31', pr_q1_2024, CAST(NULL AS STRING),
    '2024-06-30', pr_q2_2024, CAST(NULL AS STRING),
    '2024-09-30', pr_q3_2024, CAST(NULL AS STRING),
    '2024-12-31', pr_q4_2024, CAST(NULL AS STRING),
    '2025-03-31', pr_q1_2025, ki_q1_2025,
    '2025-06-30', pr_q2_2025, ki_q2_2025,
    '2025-09-30', pr_q3_2025, ki_q3_2025,
    '2025-12-31', pr_q4_2025, ki_q4_2025,
    '2026-03-31', pr_q1_2026, ki_q1_2026,
    '2026-06-30', pr_q2_2026, ki_q2_2026,
    '2026-09-30', pr_q3_2026, ki_q3_2026,
    '2026-12-31', pr_q4_2026, ki_q4_2026
) st AS eval_date_str, raw_score, raw_ki

WHERE raw_score IS NOT NULL 
  AND TRIM(CAST(raw_score AS STRING)) <> ''
  AND employee_code IS NOT NULL
  AND TRIM(CAST(employee_code AS STRING)) <> ''
""")

df_resigned.createOrReplaceTempView("tmp_hr_performance_resigned")


# ==============================================================================
# 5. TMP UNION
# ==============================================================================

df_union = spark.sql("""
SELECT * FROM tmp_hr_performance_onboard
UNION ALL
SELECT * FROM tmp_hr_performance_resigned
""")

df_union.createOrReplaceTempView("tmp_hr_performance_union")


# ==============================================================================
# 6. DEDUP
#    Uu tien onboard_logs truoc, resigned_logs sau.
# ==============================================================================
window_spec = Window.partitionBy(
    "employee_code",
    "evaluation_date"
).orderBy(
    F.desc("source_priority"),
    F.when(F.col("crawled_at_sort").isNull(), 0).otherwise(1).desc(),
    F.desc("crawled_at_sort")
)

df_dedup = (
    df_union.withColumn("rn", F.row_number().over(window_spec))
            .filter(F.col("rn") == 1)
            .drop("rn", "source_priority", "crawled_at_sort")
)

df_dedup.createOrReplaceTempView("tmp_hr_performance_dedup")


# ==============================================================================
# 7. ADD PERFORMANCE_RESULT
# ==============================================================================

df_final = (
    df_dedup
    .withColumn(
        "performance_result",
        F.when(F.col("score") < 250, F.lit("Không đạt"))
         .when((F.col("score") >= 250) & (F.col("score") <= 349), F.lit("Đạt"))
         .when(F.col("score") >= 350, F.lit("Vượt yêu cầu"))
         .otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "latest_status",
        F.when(F.col("termination_date").isNotNull(), F.lit("Resigned"))
         .otherwise(F.lit("Active"))
    )
    .select(
        "employee_code",
        "employee_name",
        "username",
        "business_email",
        "gender",
        "age_group",
        "contract_type",
        "resource_type",
        "hire_date_vcs",
        "latest_status",
        "division_n",
        "department_n_1",
        "department_n_2",
        "department_n_3",
        "branch",
        "tenure",
        "tenure_group",
        "job_level",
        "position_level",
        "competency_zone",
        "role_base",
        "evaluation_date",
        "score",
        "performance_result",
        "ki",
        "termination_date",
        "crawled_at_ts"
    )
)
df_final.createOrReplaceTempView("tmp_hr_performance_final")


# ==============================================================================
# 8. WRITE TARGET
# ==============================================================================

spark.conf.set("spark.sql.hive.convertMetastoreParquet", "true")

(
    df_final.write
    .mode("overwrite")
    .format("parquet")
    .option("path", tgt_path)
    .saveAsTable(tgt_table)
)

spark.catalog.refreshTable(tgt_table)


# ==============================================================================
# 9. VALIDATE TARGET
# ==============================================================================

print("--- Thong tin bang Target: {} ---".format(tgt_table))

spark.sql("SELECT COUNT(*) AS cnt FROM {}".format(tgt_table)).show()

spark.sql("""
SELECT
    employee_code,
    evaluation_date,
    COUNT(*) AS cnt
FROM {}
GROUP BY employee_code, evaluation_date
HAVING COUNT(*) > 1
LIMIT 20
""".format(tgt_table)).show(truncate=False)

spark.sql("""
SELECT 
    evaluation_date,
    performance_result,
    COUNT(*) AS cnt
FROM {}
GROUP BY evaluation_date, performance_result
ORDER BY evaluation_date, performance_result
""".format(tgt_table)).show(200, truncate=False)

spark.sql("""
SELECT *
FROM {}
LIMIT 10
""".format(tgt_table)).show(truncate=False)