%livy.pyspark

# Source & Target
src_hr_table = "hr_raw.hr_training_budget_expense_detail"
tgt_hr_table = "bi_silver.hr_ld_budget_expense"
tgt_hr_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_ld_budget_expense"

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
    CAST(sequence_number AS BIGINT) AS id,
    '2026' AS year,
    expense_description,
    expense_category,
    CAST(quarter AS BIGINT) AS quarter,
    CAST(proposal_amount AS DECIMAL(18,2)) AS proposal_amount,
    CAST(accounted_amount AS DECIMAL(18,2)) AS accounted_amount,
    CAST(amount_difference AS DECIMAL(18,2)) AS amount_difference,
    CAST(settlement_date AS TIMESTAMP) AS settlement_date,
    note
FROM hr_raw_view
WHERE TRIM(expense_description) IS NOT NULL
  AND TRIM(expense_description) <> ''
  AND snapshot_date_ts = (
      SELECT MAX(snapshot_date_ts)
      FROM hr_raw_view
  )
""".format(tgt_hr_table, tgt_hr_path)

spark.sql(sql_query)

# Refresh target
spark.sql("REFRESH TABLE " + tgt_hr_table)

print("--- SUCCESS: Created table ---")