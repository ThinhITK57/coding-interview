# %livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_employee_certificate_list"
tgt_hr_table = "bi_silver.hr_ld_employee_certification"
tgt_hr_path = "s3a://bi-silver/hr_ld_employee_certification"

# Refresh source
spark.sql("REFRESH TABLE " + src_hr_table)

# Load source
df_hr = spark.table(src_hr_table)
df_hr.createOrReplaceTempView("hr_raw_view")

# Create silver 
sql_query = """
CREATE TABLE IF NOT EXISTS {0}
USING PARQUET
LOCATION '{1}'
AS
SELECT
    CAST(
        CONV(SUBSTRING(MD5(CONCAT_WS('|',
            COALESCE(TRIM(employee_code), ''),
            COALESCE(TRIM(certificate_name), ''),
            COALESCE(TRIM(certificate_level), ''),
            COALESCE(CAST(certificate_issue_date AS STRING), ''),
            COALESCE(CAST(certificate_expiry_date AS STRING), '')
        )), 1, 15), 16, 10)
    AS BIGINT) AS id,

    CAST(month AS BIGINT) AS month,
    CAST(year AS BIGINT) AS year,
    TRIM(employee_code) AS employee_code,
    TRIM(full_name) AS full_name,
    division_n,
    department_n_2,
    role_base,
    level,
    employee_object,
    certificate_check_flag,
    certificate_status,
    certificate_name,
    certificate_level,
    certificate_result,
    certificate_type,
    CAST(certificate_issue_date AS TIMESTAMP) AS certificate_issue_date,
    certificate_validity_period,
    CAST(certificate_expiry_date AS TIMESTAMP) AS certificate_expiry_date,
    certificate_current_status
FROM hr_raw_view
WHERE snapshot_date_ts = (
    SELECT MAX(snapshot_date_ts)
    FROM hr_raw_view
)
""".format(tgt_hr_table, tgt_hr_path)

spark.sql(sql_query)

# Refresh target
spark.sql("REFRESH TABLE " + tgt_hr_table)

print("--- SUCCESS: Created table ---")