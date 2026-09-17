%livy.pyspark

target_table = "bi_gold.cx_sur_fact_answers"
target_path  = "/opt/datasets/crawlers/vcs/bi_gold/data/cx_sur_fact_answers"

# hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# spark.sql(f"DROP TABLE IF EXISTS {target_table}")

sql_query = """
WITH r_norm AS (
  SELECT
    CAST(collected_date AS TIMESTAMP) AS collected_ts,
    CAST(collected_date AS DATE)      AS collected_date,
    CAST(survey_id AS STRING)         AS survey_id,
    response_id,
    respondent_uuid,
    CAST(question_id AS STRING)       AS question_id,
    question_type,
    CAST(choice_id AS STRING)         AS choice_id,
    answer_string,
    answer_number,
    answer_tag,
    comment,
    choice_content,
    item_content,
    item_score
  FROM cx_survicate_bronze.responses_envelope_parse
),
qac_norm AS (
  SELECT
    CAST(survey_id AS STRING)        AS survey_id,
    CAST(question_id AS STRING)      AS question_id,
    CAST(answer_choice_id AS STRING) AS answer_choice_id,
    kpi_type,
    journey,
    customer_touchpoint,
    product_category
  FROM cx_survicate_bronze.question_answer_choices
)
SELECT
  r.collected_ts,
  r.collected_date,
  r.survey_id,
  r.response_id,
  r.respondent_uuid,
  r.question_id,
  r.question_type,
  r.choice_id,
  r.answer_string,
  r.answer_number,
  r.answer_tag,
  r.comment,
  r.choice_content,
  r.item_content,
  r.item_score,
  q.kpi_type,
  q.journey,
  q.customer_touchpoint,
  q.product_category
FROM r_norm r
LEFT JOIN qac_norm q
  ON  r.survey_id   = q.survey_id
  AND r.question_id = q.question_id
  AND r.choice_id   = q.answer_choice_id
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .partitionBy("collected_date")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")
