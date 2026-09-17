# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS cx_survicate_silver.question_columns")

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
    c           AS column_value
FROM last_items_cte q
LATERAL VIEW OUTER explode(q.columns) c_tbl AS c
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/question_columns"
    ) \
    .saveAsTable("cx_survicate_silver.question_columns")
