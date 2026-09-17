# %livy.pyspark
from pyspark.sql.types import StringType
import re

target_table = "bi_silver.cx_sur_question_response"
target_path  = "s3a://bi-silver/cx_sur_question_response"

# 3. Clean up existing target to prevent metadata mismatch
# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))
spark.catalog.clearCache()
spark.sql("REFRESH TABLE cx_survicate_raw.dim_survicate_questions")
spark.sql("REFRESH TABLE cx_survicate_raw.survey_question_master")
spark.sql("REFRESH TABLE cx_survicate_raw.fact_survicate_responses")
spark.sql("REFRESH TABLE cx_cso_silver.cso_cx_company")




def extract_email(url):
    if url is None:
        return None

    url = url.lower().replace("&amp;", "&")

    url = url.replace("%40", "@") \
             .replace("%2b", "+") \
             .replace("%2e", ".")

    match = re.search(r'([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})', url)

    return match.group(1) if match else None


def extract_ticket_id(url):
    if url is None:
        return None

    url = url.replace("&amp;", "&")

    match = re.search(r"(?i)(?:\?|&)ticket=([^&]+)", url)
    if match:
        value = match.group(1).strip()
        match_id = re.search(r"\d{5}", value)
        if match_id:
            return match_id.group(0)

    match = re.search(r"(?i)(?:\?|&)id=([^&]+)", url)
    if match:
        value = match.group(1).strip()
        match_id = re.search(r"\d{5}", value)
        if match_id:
            return match_id.group(0)

    return None

spark.udf.register("extract_email", extract_email, StringType())
spark.udf.register("extract_ticket_id", extract_ticket_id, StringType())


answer_choices_src = spark.sql("""
    SELECT
        CAST(q.survey_id AS STRING) AS survey_id,
        CAST(q.id AS STRING) AS question_id,
        COALESCE(CAST(ac.id AS STRING) , '-1') AS answer_choice_id,
        q.type AS question_type,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(q.question, '(?i)<br\\s*/?>', '\n'),
                    '<[^>]*>',
                    ''
                ),
                '&amp;',
                '&'
            )
        ) AS question_clean,
        ac.content AS answer_choice_content,
        CAST(q.crawled_at_ts AS TIMESTAMP) AS crawled_at_ts
    FROM cx_survicate_raw.dim_survicate_questions q
    LATERAL VIEW OUTER explode(q.answer_choices) ac_tbl AS ac
""")
answer_choices_src.createOrReplaceTempView("answer_choices_src")

survey_choice_src = spark.sql("""
    WITH product_excels AS (
        SELECT
            survey_id,
            question_id,
            CASE WHEN answer_choice_id IS NULL OR answer_choice_id = '' THEN '-1'
                 ELSE CAST(CAST(CAST(answer_choice_id AS DOUBLE) AS BIGINT) AS STRING)
            END AS answer_choice_id,
            CASE WHEN kpi = '' THEN NULL ELSE kpi END AS kpi,
            question_type,
            product_service,
            diem_cham_survey AS customer_touchpoint,
            journey
        FROM cx_survicate_raw.survey_question_master
        WHERE active = 'Yes'
    ),

    ranked AS (
        SELECT
            ac.answer_choice_id,
            ac.question_id,
            ac.survey_id,
            LOWER(COALESCE(pe.kpi, pe.question_type, ac.question_type)) AS question_type,
            pe.product_service AS product_category_by_question,
            ac.question_clean,
            ac.answer_choice_content,
            pe.customer_touchpoint,
            pe.journey,
            pe.kpi AS excel_kpi,
            ROW_NUMBER() OVER (
                PARTITION BY ac.survey_id, ac.question_id, ac.answer_choice_id
                ORDER BY ac.crawled_at_ts DESC
            ) AS rn
        FROM answer_choices_src ac
        LEFT JOIN product_excels pe
            ON ac.survey_id = pe.survey_id
            AND ac.question_id = pe.question_id
        WHERE ac.answer_choice_id IS NOT NULL
            AND ac.question_id IS NOT NULL
            AND ac.survey_id IS NOT NULL
    )

    SELECT
        answer_choice_id,
        question_id,
        survey_id,
        question_type,
        product_category_by_question,
        question_clean,
        answer_choice_content,
        customer_touchpoint,
        journey,
        excel_kpi
    FROM ranked
    WHERE rn = 1
""")

survey_choice_src.createOrReplaceTempView("survey_choice_src")

# Đã sửa lỗi dấu phẩy thừa và mang thêm excel_kpi làm cột fallback chống NULL kpi_type
question_answer_choices_src = spark.sql("""
    SELECT 
        bd.*,
        CASE 
            WHEN LOWER(bd.question_type) like '%nps%' THEN 'nps' 
            WHEN LOWER(bd.question_type) like 'ces%' THEN 'ces' 
            WHEN LOWER(bd.question_type) like '%csat%' THEN 'csat' 
            ELSE bd.excel_kpi
        END AS kpi_type
    FROM survey_choice_src bd
""")
question_answer_choices_src.createOrReplaceTempView("question_answer_choices_src")

response_lvl1_src = spark.sql("""
WITH responses_envelope AS (
    SELECT
    survey_id,
    response.uuid               AS uuid,
    response.url                AS url,
    response.device_type        AS device_type,
    response.operating_system   AS operating_system,
    response.language           AS language,
    response.platform           AS platform,
    extract_ticket_id(response.url)     AS cso_ticket_id,
    response.cso_uid            AS cso_uid,
    response.cso_email          AS cso_email,
    extract_email(response.url) AS url_email,
    response.respondent         AS respondent,
    response.respondent.uuid    AS respondent_uuid,
    response.answers            AS answers,
     COALESCE(
      to_timestamp(regexp_replace(response.collected_at, 'Z$', ''), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
      to_timestamp(regexp_replace(response.collected_at, 'Z$', ''), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    ) AS collected_at_ts,
    to_date(response.collected_at)       AS collected_at
    FROM cx_survicate_raw.fact_survicate_responses
)
 SELECT
        b.survey_id,
        b.uuid AS response_id,
        b.respondent_uuid,
        b.cso_ticket_id,
        COALESCE(b.url_email, b.cso_email) as response_email,
        CAST(b.collected_at AS DATE) AS collected_date,
        a.question_id,
        LOWER(a.question_type) AS  question_type,
        CAST(a.answer AS STRING) AS answer_raw_str,
        a.answers AS answers_array,
        a.fields  AS fields_array,
        a.comment AS question_comment,
        a AS raw_answer
    FROM responses_envelope b
    LATERAL VIEW OUTER explode(b.answers) t AS a
""")
response_lvl1_src.createOrReplaceTempView("response_lvl1_src")

company_contact_src = spark.sql("""
    select
        contact_email,
        contact_level,
        company_name,
        company_group,
        company_segement_level1,
        company_segement_level2,
        company_segement_level3
    FROM (
        select
            contact_email,
            contact_level,
            company_name,
            company_group,
            company_segement_level1,
            company_segement_level2,
            company_segement_level3,
            ROW_NUMBER() OVER (
                PARTITION BY contact_email
                ORDER BY company_name
            ) AS rn
        FROM cx_cso_silver.cso_cx_company
        WHERE contact_email IS NOT NULL
    ) t
    WHERE rn = 1
""")
company_contact_src.createOrReplaceTempView("company_contact_src")

sql_query = r"""
WITH response_parsed_scalar AS (
  SELECT
    *,
    NULLIF(regexp_extract(answer_raw_str, ', comment=([^,}]+)', 1), 'None') AS outer_comment,
    NULLIF(regexp_extract(answer_raw_str, '[\'"]?comment[\'"]?[:= ]+[\'"]?([^,\'"}]+)[\'"]?', 1), 'None') AS inner_comment,
    NULLIF(regexp_extract(answer_raw_str, 'id[\'"]?[:= ]+[\'"]?([^,\'"}]+)[\'"]?', 1), '') AS ans_id,
    NULLIF(regexp_extract(answer_raw_str, 'content[\'"]?[:= ]+[\'"]?([^,\'"}]+)[\'"]?', 1), '') AS ans_content,
    NULLIF(regexp_extract(answer_raw_str, 'rating[\'"]?[:= ]+[\'"]?([0-9.]+)[\'"]?', 1), '') AS ans_rating,
    NULLIF(regexp_extract(answer_raw_str, 'tag[\'"]?[:= ]+[\'"]?([^,\'"}]+)[\'"]?', 1), '') AS ans_tag
  FROM response_lvl1_src
),
responses_envelope_parse AS (
    SELECT
        survey_id, response_id, respondent_uuid, cso_ticket_id, response_email, collected_date,
        question_id, 0 AS answers_item_index, question_type,
        CAST(ans_id AS BIGINT) AS choice_id, ans_content AS answer_string,
        CAST(COALESCE(ans_rating, IF(ans_content REGEXP '^[0-9.]+$', ans_content, NULL)) AS DOUBLE) AS answer_number,
        ans_tag AS answer_tag, COALESCE(outer_comment, inner_comment, CAST(question_comment AS STRING)) AS comment,
        CAST(NULL AS STRING) AS choice_content, CAST(NULL AS STRING) AS item_content,
        CAST(NULL AS STRING) AS item_score, CAST(NULL AS INT) AS position, raw_answer
    FROM response_parsed_scalar
    WHERE question_type IN ('rating', 'csat', 'nps', 'numerical_scale', 'smiley_scale', 'single', 'text')
    UNION ALL
    SELECT
        x.survey_id, x.response_id, x.respondent_uuid, x.cso_ticket_id, x.response_email, x.collected_date,
        x.question_id, idx AS answers_item_index, x.question_type, CAST(c.id AS BIGINT) AS choice_id,
        CAST(NULL AS STRING) AS answer_string, CAST(NULL AS DOUBLE) AS answer_number, CAST(NULL AS STRING) AS answer_tag,
        CAST(COALESCE(c.comment, x.question_comment) AS STRING) AS comment, CAST(c.content AS STRING) AS choice_content,
        CAST(c.content AS STRING) AS item_content, CAST(COALESCE(c.score, CAST(c.rank AS STRING)) AS STRING) AS item_score,
        CAST(c.rank AS INT) AS position, x.raw_answer
    FROM response_lvl1_src x LATERAL VIEW OUTER posexplode(x.answers_array) t2 AS idx, c
    WHERE x.question_type IN ('multiple', 'matrix', 'ranking')
    UNION ALL
    SELECT
        x.survey_id, x.response_id, x.respondent_uuid, x.cso_ticket_id, x.response_email, x.collected_date,
        x.question_id, idx AS answers_item_index, x.question_type, CAST(NULL AS BIGINT) AS choice_id,
        CAST(f.content AS STRING) AS answer_string, CAST(NULL AS DOUBLE) AS answer_number, CAST(NULL AS STRING) AS answer_tag,
        CAST(x.question_comment AS STRING) AS comment, CAST(NULL AS STRING) AS choice_content,
        CAST(f.type AS STRING) AS item_content, CAST(NULL AS STRING) AS item_score, CAST(NULL AS INT) AS position, x.raw_answer
    FROM response_lvl1_src x LATERAL VIEW OUTER posexplode(x.fields_array) tf AS idx, f
    WHERE x.question_type = 'form'
),
r_norm_src AS (
    SELECT
        CAST(r.survey_id AS STRING)         AS survey_id,
        CAST(question_id AS STRING)       AS question_id,
        COALESCE(CAST(choice_id AS STRING) , '-1') AS choice_id,
        response_id, respondent_uuid, answers_item_index,
        TO_TIMESTAMP(CAST(collected_date AS DATE))      AS collected_date,
        CAST(cso_ticket_id AS STRING)   AS cso_ticket_id,
        CAST(response_email AS STRING)   AS response_email,
        CASE WHEN question_type = '' THEN 'N/A' ELSE COALESCE(question_type, 'N/A') END as question_type,
        CASE WHEN answer_string = '' THEN 'N/A' ELSE COALESCE(answer_string, 'N/A') END as answer_string,
        CASE WHEN answer_tag = '' THEN 'N/A' ELSE COALESCE(answer_tag, 'N/A') END as answer_tag,
        CASE WHEN comment = '' THEN 'N/A' ELSE COALESCE(comment, 'N/A') END as comment,
        CASE WHEN choice_content = '' THEN 'N/A' ELSE COALESCE(choice_content, 'N/A') END as choice_content,
        CASE WHEN item_content = '' THEN 'N/A' ELSE COALESCE(item_content, 'N/A') END as item_content,
        CASE WHEN answer_number is null THEN 0 ELSE COALESCE(CAST(answer_number as DOUBLE), 0) END as answer_number,
        CASE
            WHEN question_type NOT IN ('rating', 'ces', 'nps')  AND question_type NOT LIKE '%csat%'  
                THEN COALESCE(CAST(answer_number as DOUBLE), 0)
            WHEN answer_number IS NOT NULL                                                           
                THEN COALESCE(CAST(answer_number as DOUBLE), 0) 
            WHEN item_score IS NOT NULL AND item_score RLIKE '^[0-9]+(\\.[0-9]+)?$'                  
                THEN CAST(TRIM(item_score) AS DOUBLE)
            WHEN REGEXP_EXTRACT(TRIM(answer_string), '^([0-9]+)', 1) <> ''                           
                THEN CAST(REGEXP_EXTRACT(TRIM(answer_string), '^([0-9]+)', 1)  AS DOUBLE)
            ELSE COALESCE(CAST(answer_number as DOUBLE), 0) 
        END AS item_score_value,
        COALESCE(item_score, 'N/A') AS item_score_label,
        CASE 
            WHEN question_type = 'ces' THEN 'CES'
            WHEN question_type = 'nps' THEN 'NPS'
            WHEN question_type LIKE '%csat%'   THEN 'CSAT'
        END as kpi_type,
        CASE 
            WHEN question_type = 'ces' THEN 'CES'
            WHEN question_type = 'nps' THEN 'NPS'
            WHEN question_type LIKE '%csat%'   THEN 'CSAT'
        END as kpi_group,
        ROW_NUMBER() OVER (
            PARTITION BY survey_id, response_id, respondent_uuid, question_id, choice_id
            ORDER BY survey_id
        ) AS rn
    FROM responses_envelope_parse r
),
qac_norm_src AS (
    SELECT
        CAST(k.survey_id AS STRING)        AS survey_id,
        CAST(k.question_id AS STRING)      AS question_id,
        CAST(k.answer_choice_id AS STRING) AS answer_choice_id,
        k.customer_touchpoint,
        LOWER(COALESCE(k.question_clean,'')) as question_clean,
        k.kpi_type,
        k.product_category_by_question,
        k.journey,
        CASE
            WHEN k.kpi_type = 'CSAT điểm chạm' THEN 'CSAT_TRANSACTIONAL'
            WHEN k.kpi_type IN ('CSAT SPDV', 'CSAT nhân sự', 'CSAT quy trình') THEN 'CSAT_RELATIONAL'
            WHEN LOWER(k.kpi_type) = 'ces' THEN 'CES'
            WHEN LOWER(k.kpi_type) RLIKE 'nps' THEN 'NPS'
            WHEN LOWER(k.kpi_type) RLIKE 'csat' THEN 'CSAT'
            WHEN k.kpi_type IN ('Nhân sự good', 'Nhân sự bad', 'Quy trình good', 'Quy trình bad', 'Đồng hành', 'Chuyên gia') THEN 'CSAT_RELATIONAL'
            WHEN k.kpi_type IN ('Hiệu năng good', 'Hiệu năng bad', 'Tính năng good', 'Tính năng bad') THEN 'CSAT_RELATIONAL'
            ELSE 'UNKNOWN'
        END AS kpi_group
    FROM question_answer_choices_src k
)
SELECT
    r.response_id,
    r.survey_id,
    r.question_id,
    r.respondent_uuid,
    r.choice_id,
    r.answers_item_index,
    r.collected_date,
    r.cso_ticket_id,
    r.response_email,
    r.question_type,
    r.answer_string,
    r.answer_tag,
    r.comment,
    r.choice_content,
    r.item_content,
    r.answer_number,
    r.item_score_value,
    r.item_score_label,
    q.question_clean,
    COALESCE(q.kpi_type, r.kpi_type) as kpi_type,
    COALESCE(q.journey, 'UNKNOWN') as journey,
    COALESCE(q.customer_touchpoint, 'UNKNOWN') as customer_touchpoint,
    COALESCE(q.product_category_by_question, 'UNKNOWN') as product_category,
    COALESCE(q.kpi_group, r.kpi_group) as kpi_group,
    CASE
        WHEN q.kpi_group = 'NPS' OR r.kpi_group = 'NPS' THEN
            CASE
                WHEN r.item_score_value BETWEEN 0 AND 6 THEN 'DETRACTORS'
                WHEN r.item_score_value BETWEEN 7 AND 8 THEN 'PASSIVES'
                WHEN r.item_score_value BETWEEN 9 AND 10 THEN 'PROMOTERS'
                ELSE 'UNKNOWN'
            END
        WHEN q.kpi_group IN ('CSAT', 'CSAT_TRANSACTIONAL', 'CSAT_RELATIONAL') OR r.kpi_group IN ('CSAT', 'CSAT_TRANSACTIONAL', 'CSAT_RELATIONAL')  THEN
            CASE CAST(r.item_score_value AS INT)
                WHEN 1 THEN 'VERY_DISSATISFIED'
                WHEN 2 THEN 'DISSATISFIED'
                WHEN 3 THEN 'NEUTRAL'
                WHEN 4 THEN 'SATISFIED'
                WHEN 5 THEN 'VERY_SATISFIED'
                ELSE 'UNKNOWN'
            END
        WHEN q.kpi_group = 'CES' OR r.kpi_group = 'CES' THEN
            CASE
                WHEN r.item_score_value BETWEEN 1 AND 3 THEN 'VERY_DIFFICULT'
                WHEN r.item_score_value BETWEEN 4 AND 5 THEN 'NEUTRAL'
                WHEN r.item_score_value BETWEEN 6 AND 7 THEN 'VERY_EASY'
                ELSE 'UNKNOWN'
            END
        ELSE 'UNKNOWN'
    END AS kpi_sub_group,
    
    cc.company_name AS customer_name,
    cc.company_group AS customer_group,
    cc.company_segement_level1 AS customer_segement_level1,
    cc.company_segement_level2 AS customer_segement_level2,
    cc.company_segement_level3 AS customer_segement_level3,
    cc.contact_level AS customer_contact_level
FROM r_norm_src r
LEFT JOIN qac_norm_src q
  ON  r.survey_id   = q.survey_id
  AND r.question_id = q.question_id
  AND r.choice_id   = q.answer_choice_id
LEFT JOIN company_contact_src cc
 ON r.response_email = cc.contact_email

WHERE r.rn = 1
"""
df = spark.sql(sql_query)


(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
   .save(target_path)
)
#   .save(target_path)
#   .option("path", target_path) \
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
