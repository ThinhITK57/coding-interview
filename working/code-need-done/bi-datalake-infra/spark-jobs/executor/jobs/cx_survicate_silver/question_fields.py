# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS cx_survicate_silver.question_fields")

sql_query = """
WITH last_items_cte AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY id DESC
            ) AS rn
        FROM cx_survicate_raw.dim_survicate_questions
    ) t
    WHERE rn = 1
)

SELECT
    q.id        AS question_id,
    q.survey_id,
    q.type      AS question_type,
    q.question,
    f.type      AS field_type,
    f.label     AS field_label
FROM last_items_cte q
LATERAL VIEW OUTER explode(q.fields) f_tbl AS f
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/question_fields"
    ) \
    .saveAsTable("cx_survicate_silver.question_fields")
