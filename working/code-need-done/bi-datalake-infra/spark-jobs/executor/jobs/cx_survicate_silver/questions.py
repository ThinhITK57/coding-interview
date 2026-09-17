# %livy.pyspark

spark.sql("drop  table IF EXISTS cx_survicate_silver.questions")
 
sql_query = """

WITH last_tems_cte AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY  id DESC
               ) rn
        FROM cx_survicate_raw.dim_survicate_questions ss
    ) t
    WHERE rn = 1
)

SELECT
    id,
    type,
    question,
    introduction,
    survey_id
--    answer_choices,
--    columns,
--    fields
FROM last_tems_cte




"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/questions"
    ) \
    .saveAsTable("cx_survicate_silver.questions")
