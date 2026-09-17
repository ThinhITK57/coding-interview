# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS cx_survicate_bronze.audit_question_types")

sql_query = """
WITH exploded AS (
  SELECT
    b.survey_id,
    b.uuid AS response_id,
    b.collected_at,

    a.question_id,
    a.question_type
  FROM cx_survicate_bronze.responses_envelope b
  LATERAL VIEW OUTER explode(b.answers) t AS a
)

SELECT
  question_type,
  COUNT(*)                      AS total_questions,
  COUNT(DISTINCT question_id)   AS distinct_question_ids,
  COUNT(DISTINCT survey_id)     AS distinct_surveys,
  MIN(collected_at)                       AS first_response_dt,
  MAX(collected_at)                       AS last_response_dt
FROM exploded
GROUP BY question_type
ORDER BY total_questions DESC
"""

df = spark.sql(sql_query)

(df.write
  .mode("overwrite")
  .format("parquet")
  .option(
      "path",
      "s3a://vcs-raw/cx-survicate-bronze/audit_question_types"
  )
  .saveAsTable("cx_survicate_bronze.audit_question_types")
)

print("✅ Created table: cx_survicate_bronze.audit_question_types")
