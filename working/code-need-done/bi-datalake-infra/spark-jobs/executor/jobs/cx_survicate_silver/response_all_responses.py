# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS cx_survicate_silver.response_all_responses")

sql_query = """
SELECT
    fr.survey_id,
    fr.response.uuid     AS response_uuid,
    ar                   AS all_response_value
FROM cx_survicate_raw.fact_survicate_responses fr
LATERAL VIEW OUTER explode(fr.all_responses) ar_tbl AS ar
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/response_all_responses"
    ) \
    .saveAsTable("cx_survicate_silver.response_all_responses")
