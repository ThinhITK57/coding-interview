%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_class_list"
tgt_hr_table = "bi_silver.hr_ld_class_list"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_class_list"

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
    month,
    year,
    CAST(updated_date AS TIMESTAMP) AS updated_date,
    organizing_unit,
    pic,
    class_code,
    class_name,
    course_name,
    training_program,
    training_method,
    program_group,
    training_format,
    class_status,
    training_duration,
    CAST(start_date AS TIMESTAMP) AS start_date,
    CAST(end_date AS TIMESTAMP) AS end_date,
    CAST(planned_learner_count AS BIGINT) AS planned_learner_count,
    CAST(actual_learner_count AS BIGINT) AS actual_learner_count,
    CAST(training_hours AS DECIMAL(10,2)) AS training_hours,
    training_content,
    instructor_name,
    CAST(organization_rating as DECIMAL(10,2)) As organization_rating,
    CAST(average_rating as DECIMAL(10,2)) As average_rating,
    level_2_evaluation,
    pass_rate,
    lxp_status_note
FROM hr_raw_view
WHERE class_name IS NOT NULL
    AND snapshot_date_ts = (
        SELECT MAX(snapshot_date_ts)
        FROM hr_raw_view
    )
""".format(tgt_hr_table, tgt_hr_path)

spark.sql(sql_query)

# Refresh target
spark.sql("REFRESH TABLE " + tgt_hr_table)

print("--- SUCCESS: Created table ---")