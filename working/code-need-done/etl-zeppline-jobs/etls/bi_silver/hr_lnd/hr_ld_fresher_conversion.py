%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_fresher_conversion_list"
tgt_hr_table = "bi_silver.hr_ld_fresher_conversion"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_fresher_conversion"

# Refresh source
spark.sql("REFRESH TABLE " + src_hr_table)

# Load source
df_hr = spark.table(src_hr_table)
df_hr.createOrReplaceTempView("hr_raw_view")

sql_query = """
CREATE TABLE IF NOT EXISTS {0}
USING PARQUET
LOCATION '{1}'
AS
SELECT
    CAST(
        CONV(SUBSTRING(MD5(CONCAT_WS('|',
            COALESCE(TRIM(employee_code), ''),
            COALESCE(TRIM(full_name), ''),
            COALESCE(CAST(onboard_date AS STRING), ''),
            COALESCE(CAST(conversion_date AS STRING), '')
        )), 1, 15), 16, 10)
    AS BIGINT) AS id,

    TRIM(full_name) AS full_name,
    TRIM(employee_code) AS employee_code,

    CAST(onboard_date AS TIMESTAMP) AS onboard_date,
    CAST(conversion_date AS TIMESTAMP) AS conversion_date,

    role_base,
    level_region,
    department_n_2,
    current_work_location,
    interview_result,
    interview_result_file

FROM hr_raw_view
WHERE COALESCE(TRIM(full_name), '') <> ''
    AND snapshot_date_ts = (
        SELECT MAX(snapshot_date_ts)
        FROM hr_raw_view
    )
""".format(tgt_hr_table, tgt_hr_path)

spark.sql(sql_query)

# Refresh target
spark.sql("REFRESH TABLE " + tgt_hr_table)

print("--- SUCCESS: Created table ---")