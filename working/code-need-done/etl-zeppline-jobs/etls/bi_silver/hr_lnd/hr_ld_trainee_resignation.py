%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_trainee_resignation_list"
tgt_hr_table = "bi_silver.hr_ld_trainee_resignation"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_trainee_resignation"

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
            COALESCE(TRIM(training_program), ''),
            COALESCE(CAST(resigned_date AS STRING), '')
        )), 1, 15), 16, 10)
    AS BIGINT) AS id,

    TRIM(full_name) AS full_name,
    TRIM(employee_code) AS employee_code,

    training_program,

    CAST(resigned_date AS TIMESTAMP) AS resigned_date,

    training_major,
    resignation_reason,

    training_phase,
    employment_type,

    CAST(expected_full_time_date AS TIMESTAMP) AS expected_full_time_date,

    TRIM(mentor_email) AS mentor_email,

    team,
    department_n_2,

    training_result_link

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