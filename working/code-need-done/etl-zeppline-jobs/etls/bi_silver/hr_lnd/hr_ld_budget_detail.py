%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_budget_summary_by_type"
tgt_hr_table = "bi_silver.hr_ld_budget_detail"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_budget_detail"

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
    '2026' AS year,
    budget_type,
    CAST(budget_amount AS DECIMAL(18,2)) AS budget_amount,
    CAST(approval_request_amount AS DECIMAL(18,2)) AS approval_request_amount,
    CAST(spent_amount AS DECIMAL(18,2)) AS spent_amount,
    CAST(remaining_amount AS DECIMAL(18,2)) AS remaining_amount
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