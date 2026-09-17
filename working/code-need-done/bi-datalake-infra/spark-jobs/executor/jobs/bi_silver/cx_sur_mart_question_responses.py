# %livy.pyspark

target_table = "bi_silver.cx_sur_mart_question_responses"
target_path  = "s3a://bi-silver/data/cx_sur_mart_question_responses"

# 3. Clean up existing target to prevent metadata mismatch
# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))
spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.cx_sur_question_response")


surveys_src = spark.sql("""
SELECT
    survey_id,
    response_id,
    respondent_uuid,
    question_id,
    response_email,
    cso_ticket_id,

    collected_date,
    CONCAT(
        YEAR(collected_date),
        'Q',
        QUARTER(collected_date)
    ) AS year_quarter,

    journey,
    customer_touchpoint,
    product_category,

    customer_name,
    customer_group,
    customer_segement_level1,
    customer_segement_level2,
    customer_segement_level3,
    customer_contact_level,

    kpi_group,
    item_score_value

FROM bi_silver.cx_sur_question_response

WHERE 1=1
    AND (
        kpi_group IN ('NPS', 'CES')
        OR kpi_group LIKE 'CSAT%'
    )
    AND response_email IS NOT NULL
    AND response_id IS NOT NULL
    AND survey_id IS NOT NULL
    AND question_id IS NOT NULL
""")
surveys_src.createOrReplaceTempView("surveys_src")


surveys_quarter_src = spark.sql("""
SELECT
        survey_id,
        response_id,
        respondent_uuid,
        question_id,
        response_email,
        cso_ticket_id,
        
        collected_date,
        journey,
        customer_touchpoint,
        product_category,

        customer_name,
        customer_group,
        customer_segement_level1,
        customer_segement_level2,
        customer_segement_level3,
        customer_contact_level,
                                
        kpi_group,
        item_score_value,
        ROW_NUMBER() OVER (
                   PARTITION BY response_email, survey_id, question_id, year_quarter
                   ORDER BY collected_date DESC
               ) rn
FROM surveys_src
WHERE 1=1
  AND customer_touchpoint = 'KS định kỳ quý'
                        
""")
surveys_quarter_src.createOrReplaceTempView("surveys_quarter_src")


surveys_ticket_src = spark.sql("""
SELECT
        survey_id,
        response_id,
        respondent_uuid,
        question_id,
        response_email,
        cso_ticket_id,
        
        collected_date,
        journey,
        customer_touchpoint,
        product_category,

        customer_name,
        customer_group,
        customer_segement_level1,
        customer_segement_level2,
        customer_segement_level3,
        customer_contact_level,

        kpi_group,
        item_score_value,
        ROW_NUMBER() OVER (
                   PARTITION BY cso_ticket_id, survey_id, question_id, year_quarter
                   ORDER BY collected_date DESC
               ) rn
FROM surveys_src
WHERE 1=1
  AND cso_ticket_id IS NOT NULL
  AND customer_touchpoint = 'KS hoàn tất xử lý ticket'
""")
surveys_ticket_src.createOrReplaceTempView("surveys_ticket_src")


surveys_other_src = spark.sql("""
SELECT
        survey_id,
        response_id,
        respondent_uuid,
        question_id,
        response_email,
        cso_ticket_id,
        
        collected_date,
        journey,
        customer_touchpoint,
        product_category,

        customer_name,
        customer_group,
        customer_segement_level1,
        customer_segement_level2,
        customer_segement_level3,
        customer_contact_level,

        kpi_group,
        item_score_value,
        ROW_NUMBER() OVER (
                   PARTITION BY response_id, survey_id, question_id, year_quarter
                   ORDER BY collected_date DESC
               ) rn
FROM surveys_src
WHERE 1=1
  AND customer_touchpoint 
        NOT IN  (
          'KS hoàn tất xử lý ticket',
          'KS định kỳ quý'
        )
""")
surveys_other_src.createOrReplaceTempView("surveys_other_src")


# 4. Core Transformation Logic
sql_query = r"""
WITH surveys_union AS (

    SELECT
        survey_id,
        response_id,
        respondent_uuid,
        question_id,
        response_email,
        cso_ticket_id,
        collected_date,
        journey,
        customer_touchpoint,
        product_category,
        customer_name,
        customer_group,
        customer_segement_level1,
        customer_segement_level2,
        customer_segement_level3,
        customer_contact_level,
        kpi_group,
        item_score_value,
        rn
    FROM surveys_quarter_src

    UNION ALL

    SELECT
        survey_id,
        response_id,
        respondent_uuid,
        question_id,
        response_email,
        cso_ticket_id,
        collected_date,
        journey,
        customer_touchpoint,
        product_category,
        customer_name,
        customer_group,
        customer_segement_level1,
        customer_segement_level2,
        customer_segement_level3,
        customer_contact_level,
        kpi_group,
        item_score_value,
        rn
    FROM surveys_ticket_src

    UNION ALL

    SELECT
        survey_id,
        response_id,
        respondent_uuid,
        question_id,
        response_email,
        cso_ticket_id,
        collected_date,
        journey,
        customer_touchpoint,
        product_category,
        customer_name,
        customer_group,
        customer_segement_level1,
        customer_segement_level2,
        customer_segement_level3,
        customer_contact_level,
        kpi_group,
        item_score_value,
        rn
    FROM surveys_other_src

)

SELECT 
  survey_id,
  response_id,
  respondent_uuid,
  question_id,
  response_email,
  cso_ticket_id,
  collected_date,
  journey,
  customer_touchpoint,
  product_category,
  customer_name,
  customer_group,
  customer_segement_level1,
  customer_segement_level2,
  customer_segement_level3,
  customer_contact_level,
  kpi_group,
  item_score_value
FROM surveys_union
WHERE rn = 1
"""

df = spark.sql(sql_query)
df.show()

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .save(target_path)
)
  # .save(target_path)
# .option("path", target_path)
#   .saveAsTable(target_table)
  
spark.catalog.refreshTable(target_table)
print(target_table)

# | kpi_group                                   | item_score_value | kpi_sub_group     |
# | ------------------------------------------- | ---------------: | ----------------- |
# | NPS                                         |              0–6 | DETRACTORS        |
# | NPS                                         |              7–8 | PASSIVES          |
# | NPS                                         |             9–10 | PROMOTERS         |
# | CSAT / CSAT_TRANSACTIONAL / CSAT_RELATIONAL |                1 | VERY_DISSATISFIED |
# | CSAT / CSAT_TRANSACTIONAL / CSAT_RELATIONAL |                2 | DISSATISFIED      |
# | CSAT / CSAT_TRANSACTIONAL / CSAT_RELATIONAL |                3 | NEUTRAL           |
# | CSAT / CSAT_TRANSACTIONAL / CSAT_RELATIONAL |                4 | SATISFIED         |
# | CSAT / CSAT_TRANSACTIONAL / CSAT_RELATIONAL |                5 | VERY_SATISFIED    |
# | CES                                         |              1–3 | VERY_DIFFICULT    |
# | CES                                         |              4–5 | NEUTRAL           |
# | CES                                         |              6–7 | VERY_EASY         |
