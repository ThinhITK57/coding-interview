# %livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_trainee_list"
tgt_hr_table = "bi_silver.hr_ld_trainee"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_trainee"

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
            COALESCE(CAST(ojt_start_date AS STRING), '')
        )), 1, 15), 16, 10)
    AS BIGINT) AS id,

    TRIM(full_name) AS full_name,
    TRIM(employee_code) AS employee_code,

    training_program,
    training_major,
    training_phase,

    CAST(ojt_start_date AS TIMESTAMP) AS ojt_start_date,
    CAST(ojt_end_date AS TIMESTAMP) AS ojt_end_date,

    ojt_progress,

    CAST(
        REPLACE(
            REGEXP_REPLACE(TRIM(average_score), '[^0-9,.-]', ''),
            ',',
            '.'
        )
        AS DECIMAL(5,2)
    ) AS average_score,

    classification,
    current_status,

    training_result_link,

    employment_type,
    work_location,

    TRIM(business_email) AS business_email,
    TRIM(mentor_email) AS mentor_email,
    CAST(snapshot_date AS TIMESTAMP) AS snapshot_date

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