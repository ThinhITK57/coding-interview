# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS cx_survicate_silver.responses")

sql_query = """
SELECT
    fr.survey_id,

    -- response struct
    fr.response.uuid                 AS response_uuid,
    fr.response.url,
    fr.response.device_type,
    fr.response.operating_system,
    fr.response.language,
    fr.response.collected_at,
    fr.response.platform,
    fr.response.cso_ticket_id,
    fr.response.cso_uid,
    fr.response.cso_email,

    -- respondent struct
    fr.response.respondent.uuid      AS respondent_uuid,
    resp_attr                        AS respondent_attribute,

    -- answer (per question)
    a.question_id,
    a.question_type,
    a.action_performed,
    -- a.answer                         AS answer_raw,
    a.translated_answer,
    a.comment,
    a.translated_comment,
    a.insight,
    a.ai_followups,
    a.translated_ai_followups
FROM cx_survicate_raw.fact_survicate_responses fr
LATERAL VIEW OUTER explode(fr.response.respondent.attributes) r_tbl AS resp_attr
LATERAL VIEW OUTER explode(fr.response.answers) a_tbl AS a
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/responses"
    ) \
    .saveAsTable("cx_survicate_silver.responses")
