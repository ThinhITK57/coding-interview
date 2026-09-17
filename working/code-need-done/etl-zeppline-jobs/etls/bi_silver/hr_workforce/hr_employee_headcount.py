%livy.pyspark

# ==============================================================================
# 1. CONFIG
# ==============================================================================

source_table = "hr_raw.hr_employee_headcount_logs"
target_table = "bi_silver.hr_employee_headcount"
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_employee_headcount"

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.catalog.clearCache()


# ==============================================================================
# 2. LOAD RAW PARQUET DIRECTLY
# ==============================================================================

print("Reading raw parquet directly...")

df_raw = spark.read.parquet(source_path)
df_raw.createOrReplaceTempView("hr_employee_headcount_raw")


# ==============================================================================
# 3. TRANSFORM
# ==============================================================================

sql_query = """
WITH base AS (
    SELECT
        *,
        COALESCE(
            NULLIF(NULLIF(TRIM(management_level), ''), '0'),
            required_level
        ) AS effective_position_level
    FROM hr_employee_headcount_raw
    WHERE snapshot_date_ts = (
        SELECT MAX(snapshot_date_ts)
        FROM hr_employee_headcount_raw
    )
)

SELECT 
    SPLIT(REGEXP_REPLACE(TRIM(headcount_plan), '\\\\s+', ' '), ' ')[1] AS plan_year,
    SPLIT(REGEXP_REPLACE(TRIM(headcount_plan), '\\\\s+', ' '), ' ')[0] AS headcount_plan_type,

    headcount_status,
    TRIM(division_n) AS division_n,
    TRIM(unit_n_1) AS department_n_1,
    TRIM(department_n_2) AS department_n_2,
    TRIM(department_n_3) AS department_n_3,
    role_base,
    specialization,
    required_level,
    position_status,
    full_name,
    employee_code,
    employee_object AS employee_type,
    management_level,

    effective_position_level,
    effective_position_level AS position_level,

    CASE
        WHEN NULLIF(TRIM(employee_object), '') IN ('TDS', 'NDS', 'HDDV') THEN 'TDS-NDS-HDDV'
        WHEN employee_object = 'CTV' THEN 'CTV'
        WHEN employee_object IN ('Fresher', 'SV') THEN 'SV+Fresher'
        WHEN employee_object IN ('IS', 'OS') THEN 'IS/OS'
        ELSE employee_object
    END AS resource_type,

    contract_type,

    CAST(
        FROM_UNIXTIME(
            UNIX_TIMESTAMP(hire_date_vcs, 'yyyy-MM-dd HH:mm:ss')
        ) AS TIMESTAMP
    ) AS hire_date_vcs,

    level AS job_level,
    region AS competency_zone,

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

    branch,
    LOWER(business_email) AS business_email,
    job_framework_position,
    job_framework_position_2,
    service_group,
    service_group_new,

    CASE WHEN is_ai_group = 'x' THEN 1 ELSE 0 END AS is_ai_group,
    CASE WHEN is_ai_org = 'x' THEN 1 ELSE 0 END AS is_ai_org,
    CASE WHEN is_philippines_japan_market = 'x' THEN 1 ELSE 0 END AS is_philippines_japan_market,
    CASE WHEN is_domestic_business = 'x' THEN 1 ELSE 0 END AS is_domestic_business,
    CASE WHEN is_international_business = 'x' THEN 1 ELSE 0 END AS is_international_business,
    CASE WHEN is_rnd = 'x' THEN 1 ELSE 0 END AS is_rnd,
    CASE WHEN is_indirect_group = 'x' THEN 1 ELSE 0 END AS is_indirect_group,
    CASE WHEN is_business_support = 'x' THEN 1 ELSE 0 END AS is_business_support

FROM base
"""

print("Executing transformation...")
df = spark.sql(sql_query)


# ==============================================================================
# 4. WRITE TARGET
# ==============================================================================

print("Dropping old target table...")
spark.sql("DROP TABLE IF EXISTS " + target_table)

print("Writing data to {}...".format(target_table))

df.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)


# ==============================================================================
# 5. VALIDATE
# ==============================================================================

print("--- SUCCESS: Created table {} ---".format(target_table))

spark.sql("""
SELECT COUNT(*) AS cnt
FROM {}
""".format(target_table)).show()

spark.sql("""
SELECT plan_year, headcount_plan_type, COUNT(*) AS cnt
FROM {}
GROUP BY plan_year, headcount_plan_type
ORDER BY plan_year, headcount_plan_type
""".format(target_table)).show(100, truncate=False)

spark.sql("""
SELECT *
FROM {}
LIMIT 10
""".format(target_table)).show(truncate=False)