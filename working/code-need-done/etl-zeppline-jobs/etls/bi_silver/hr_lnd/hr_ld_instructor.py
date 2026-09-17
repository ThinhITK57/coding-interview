%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_instructor_list"
tgt_hr_table = "bi_silver.hr_ld_instructor"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_instructor"

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
            COALESCE(TRIM(class_code), ''),
            COALESCE(CAST(start_date AS STRING), '')
        )), 1, 15), 16, 10)
    AS BIGINT) AS id,

    TRIM(employee_code) AS employee_code,
    TRIM(full_name) AS full_name,
    TRIM(business_email) AS business_email,

    instructor_group,
    division_n,
    department_n_2,

    class_code,
    class_name,

    CAST(teaching_duration AS DOUBLE) AS teaching_duration,
    CAST(instructor_fee AS DECIMAL(18,2)) AS instructor_fee,
    CAST(instructor_rating AS DECIMAL(5,2)) AS instructor_rating,

    CAST(start_date AS TIMESTAMP) AS start_date,
    CAST(end_date AS TIMESTAMP) AS end_date,

    CAST(month AS INTEGER) AS month,
    CAST(year AS INTEGER) AS year

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