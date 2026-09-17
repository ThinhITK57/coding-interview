%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_trainee_conversion_list"
tgt_hr_table = "bi_silver.hr_ld_trainee_conversion"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_trainee_conversion"

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
            COALESCE(TRIM(trainee_id), ''),
            COALESCE(TRIM(employee_code), ''),
            COALESCE(CAST(conversion_date AS STRING), '')
        )), 1, 15), 16, 10)
    AS BIGINT) AS id,

    TRIM(full_name) AS full_name,
    TRIM(trainee_id) AS trainee_id,
    TRIM(employee_code) AS employee_code,

    training_program,

    CAST(conversion_date AS TIMESTAMP) AS conversion_date,

    role_base,
    level_region,
    training_phase,
    employment_type,

    CAST(expected_full_time_date AS TIMESTAMP) AS expected_full_time_date,

    training_major,
    TRIM(mentor_email) AS mentor_email,

    team,
    department_n_2,

    interview_round_2_result,
    training_result_link,

    CAST(
        REPLACE(
            REGEXP_REPLACE(TRIM(training_score), '[^0-9,.-]', ''),
            ',',
            '.'
        )
        AS DECIMAL(5,2)
    ) AS training_score

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