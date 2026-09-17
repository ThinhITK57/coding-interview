# %livy.pyspark

target_table = "bi_silver.cx_sur_question_answer_choices"
target_path  = "s3a://bi-silver/cx_sur_question_answer_choices"

# Hard delete path (avoid leftover files)
# jvm = spark._jvm
# hconf = spark._jsc.hadoopConfiguration()
# fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# spark.sql(f"DROP TABLE IF EXISTS {target_table}")
spark.catalog.clearCache()

sql_query = r"""
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY id DESC   -- TODO: thay bằng cột thể hiện "mới nhất"
        ) AS rn
    FROM cx_survicate_raw.dim_survicate_questions
),
last_items_cte AS (
    SELECT * FROM ranked WHERE rn = 1
),
base_data AS (
    SELECT
        CAST(q.survey_id AS STRING)  AS survey_id,
        CAST(q.id AS STRING)         AS question_id,
        q.type                       AS question_type,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(q.question, '<br\\s*/?>', '\n'),
                    '<[^>]*>',
                    ''
                ),
                '&amp;',
                '&'
            )
        ) AS question_clean,
        CAST(ac.id AS STRING)        AS answer_choice_id,
        ac.content                   AS answer_choice_content
    FROM last_items_cte q
    LATERAL VIEW OUTER explode(q.answer_choices) ac_tbl AS ac
),
qcj_norm AS (
    SELECT
        CAST(survey_id AS STRING)        AS survey_id,
        CAST(question_id AS STRING)      AS question_id,
        CAST(answer_choice_id AS STRING) AS answer_choice_id,
        kpi
    FROM cx_cso_raw.dim_question_customer_journey
),
scj_norm AS (
    SELECT
        CAST(survey_id AS STRING) AS survey_id,
        journey,
        customer_touchpoint,
        product_category
    FROM cx_cso_raw.dim_surveys_customer_journey
)
SELECT
    bd.*,
    CASE 
        WHEN bd.question_type = 'nps' THEN 'NPS' 
        ELSE COALESCE(qcj.kpi, 'N/A') 
    END AS kpi_type,
    COALESCE(scj.journey, 'N/A') as journey,
    COALESCE(scj.customer_touchpoint,'N/A') as customer_touchpoint,
    COALESCE(scj.product_category, 'N/A') as product_category
FROM base_data bd
LEFT JOIN qcj_norm qcj
  ON  bd.survey_id = qcj.survey_id
  AND bd.question_id = qcj.question_id
  AND bd.answer_choice_id = qcj.answer_choice_id
LEFT JOIN scj_norm scj
  ON bd.survey_id = scj.survey_id
"""

df = spark.sql(sql_query)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)
    # .option("path", target_path) \
    # .saveAsTable(target_table)
    
spark.catalog.refreshTable(target_table)
print(target_table)
#print(f"DONE: {target_table} saved at {target_path}")
