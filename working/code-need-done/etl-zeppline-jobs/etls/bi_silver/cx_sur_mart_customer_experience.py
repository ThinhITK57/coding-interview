%livy.pyspark
from pyspark.sql import functions as F

target_path = "bi_silver.cx_sur_question_answer_choices"
target_table = "bi_silver.cx_sur_mart_customer_experience"

spark.catalog.clearCache()

spark.sql("REFRESH TABLE bi_silver.cx_sur_mart_question_responses")

# 3) Query
sql_query = """
WITH response_score AS (

    SELECT
        survey_id,
        response_id,
        respondent_uuid,
        
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

        MAX(CASE
            WHEN kpi_group = 'NPS'
            THEN item_score_value
        END) AS customer_nps_score,

        MAX(CASE
            WHEN kpi_group LIKE 'CSAT%'
            THEN item_score_value
        END) AS customer_csat_score,

        MAX(CASE
            WHEN kpi_group = 'CES'
            THEN item_score_value
        END) AS customer_ces_score

    FROM bi_silver.cx_sur_mart_question_responses
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),

classified AS (

SELECT
    *,

    --------------------------------------------------------------------
    -- CX Rank
    --------------------------------------------------------------------

    CASE

        WHEN customer_nps_score BETWEEN 9 AND 10
        AND customer_csat_score IN (4,5)
        AND customer_ces_score IN (6,7)
            THEN 1

        WHEN customer_nps_score BETWEEN 9 AND 10
        AND customer_csat_score IN (4,5)
        AND customer_ces_score < 4
            THEN 2

        WHEN (
                customer_nps_score BETWEEN 7 AND 8
            OR customer_csat_score = 3
            )
        AND customer_ces_score IN (6,7)
            THEN 3

        WHEN customer_nps_score IS NOT NULL
        AND customer_csat_score IS NOT NULL
        AND customer_ces_score IS NOT NULL
        AND NOT (
                (customer_nps_score BETWEEN 9 AND 10 AND customer_csat_score IN (4,5) AND customer_ces_score IN (6,7))
            OR (customer_nps_score BETWEEN 9 AND 10 AND customer_csat_score IN (4,5) AND customer_ces_score < 4)
            OR ((customer_nps_score BETWEEN 7 AND 8 OR customer_csat_score = 3) AND customer_ces_score IN (6,7))
            OR ((customer_nps_score BETWEEN 7 AND 8 OR customer_csat_score = 3) AND customer_ces_score < 4)
            OR ((customer_nps_score < 7 OR customer_csat_score IN (1,2)) AND customer_ces_score IN (6,7))
            OR ((customer_nps_score < 7 OR customer_csat_score IN (1,2)) AND customer_ces_score < 4)
        )
            THEN 4

        WHEN (
                customer_nps_score BETWEEN 7 AND 8
            OR customer_csat_score = 3
            )
        AND customer_ces_score < 4
            THEN 5

        WHEN (
                customer_nps_score < 7
            OR customer_csat_score IN (1,2)
            )
        AND customer_ces_score IN (6,7)
            THEN 6

        WHEN (
                customer_nps_score < 7
            OR customer_csat_score IN (1,2)
            )
        AND customer_ces_score < 4
            THEN 7

        ELSE 99

    END AS customer_experience_rank,

    --------------------------------------------------------------------
    -- CX Label
    --------------------------------------------------------------------

    CASE

        WHEN customer_nps_score BETWEEN 9 AND 10
        AND customer_csat_score IN (4,5)
        AND customer_ces_score IN (6,7)
            THEN 'Nhóm 1. Ưu việt'

        WHEN customer_nps_score BETWEEN 9 AND 10
        AND customer_csat_score IN (4,5)
        AND customer_ces_score < 4
            THEN 'Nhóm 2. Gắn kết'

        WHEN (
                customer_nps_score BETWEEN 7 AND 8
            OR customer_csat_score = 3
            )
        AND customer_ces_score IN (6,7)
            THEN 'Nhóm 3. Ổn định'

        WHEN customer_nps_score IS NOT NULL
        AND customer_csat_score IS NOT NULL
        AND customer_ces_score IS NOT NULL
        AND NOT (
                (customer_nps_score BETWEEN 9 AND 10 AND customer_csat_score IN (4,5) AND customer_ces_score IN (6,7))
            OR (customer_nps_score BETWEEN 9 AND 10 AND customer_csat_score IN (4,5) AND customer_ces_score < 4)
            OR ((customer_nps_score BETWEEN 7 AND 8 OR customer_csat_score = 3) AND customer_ces_score IN (6,7))
            OR ((customer_nps_score BETWEEN 7 AND 8 OR customer_csat_score = 3) AND customer_ces_score < 4)
            OR ((customer_nps_score < 7 OR customer_csat_score IN (1,2)) AND customer_ces_score IN (6,7))
            OR ((customer_nps_score < 7 OR customer_csat_score IN (1,2)) AND customer_ces_score < 4)
        )
            THEN 'Nhóm 4. Trung lập'

        WHEN (
                customer_nps_score BETWEEN 7 AND 8
            OR customer_csat_score = 3
            )
        AND customer_ces_score < 4
            THEN 'Nhóm 5. Rào cản'

        WHEN (
                customer_nps_score < 7
            OR customer_csat_score IN (1,2)
            )
        AND customer_ces_score IN (6,7)
            THEN 'Nhóm 6. Rủi ro'

        WHEN (
                customer_nps_score < 7
            OR customer_csat_score IN (1,2)
            )
        AND customer_ces_score < 4
            THEN 'Nhóm 7. Khủng hoảng'

        ELSE 'Chưa phân loại'

    END AS customer_experience_label,

    -------------------------------------------------------------------
    -- đủ dữ liệu
    -------------------------------------------------------------------

    CASE
        WHEN customer_nps_score IS NOT NULL
         AND customer_csat_score IS NOT NULL
         AND customer_ces_score IS NOT NULL
        THEN 1
        ELSE 0
    END AS has_complete_cx_score,

    -------------------------------------------------------------------
    -- segment
    -------------------------------------------------------------------

    CASE

        WHEN customer_nps_score BETWEEN 9 AND 10
         AND customer_csat_score IN (4,5)
         AND customer_ces_score IN (6,7)
            THEN 'EXCELLENT'

        WHEN customer_nps_score BETWEEN 9 AND 10
         AND customer_csat_score IN (4,5)
         AND customer_ces_score <4
            THEN 'ENGAGED'

        WHEN (
                customer_nps_score BETWEEN 7 AND 8
             OR customer_csat_score =3
             )
         AND customer_ces_score IN (6,7)
            THEN 'STABLE'

        WHEN (
                customer_nps_score BETWEEN 7 AND 8
             OR customer_csat_score =3
             )
         AND customer_ces_score <4
            THEN 'BARRIER'

        WHEN (
                customer_nps_score <7
             OR customer_csat_score IN (1,2)
             )
         AND customer_ces_score IN (6,7)
            THEN 'AT_RISK'

        WHEN (
                customer_nps_score <7
             OR customer_csat_score IN (1,2)
             )
         AND customer_ces_score <4
            THEN 'CRITICAL'

        WHEN customer_nps_score IS NOT NULL
         AND customer_csat_score IS NOT NULL
         AND customer_ces_score IS NOT NULL
            THEN 'NEUTRAL'

        ELSE 'UNCLASSIFIED'

    END AS customer_experience_segment,

    -------------------------------------------------------------------
    -- sentiment
    -------------------------------------------------------------------

    CASE

        WHEN customer_nps_score >=9
         AND customer_csat_score>=4
            THEN 'POSITIVE'

        WHEN customer_nps_score<7
          OR customer_csat_score<=2
            THEN 'NEGATIVE'

        WHEN customer_nps_score IS NOT NULL
         AND customer_csat_score IS NOT NULL
            THEN 'NEUTRAL'

        ELSE 'UNKNOWN'

    END AS customer_experience_sentiment,

    -------------------------------------------------------------------
    -- score
    -------------------------------------------------------------------

    (
        CASE
            WHEN customer_nps_score BETWEEN 9 AND 10 THEN 2
            WHEN customer_nps_score BETWEEN 7 AND 8 THEN 1
            WHEN customer_nps_score BETWEEN 0 AND 6 THEN -2
            ELSE 0
        END

        +

        CASE
            WHEN customer_csat_score IN (4,5) THEN 2
            WHEN customer_csat_score=3 THEN 0
            WHEN customer_csat_score IN (1,2) THEN -2
            ELSE 0
        END

        +

        CASE
            WHEN customer_ces_score IN (6,7) THEN 2
            WHEN customer_ces_score IN (4,5) THEN 0
            WHEN customer_ces_score<=3 THEN -2
            ELSE 0
        END

    ) AS customer_experience_score
    

FROM response_score

)

SELECT *
FROM classified
"""

df = spark.sql(sql_query)

# 4) Write (Spark 2.0 stable)
df.write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", target_path) \
  .saveAsTable(target_table)
#   .save(target_path)
  

spark.catalog.refreshTable(target_table)

print("DONE. Rows:", df.count())



# | customer_experience_segment | Label               |
# | --------------------------- | ------------------- |
# | EXCELLENT                   | Nhóm 1. Ưu việt     |
# | ENGAGED                     | Nhóm 2. Gắn kết     |
# | STABLE                      | Nhóm 3. Ổn định     |
# | NEUTRAL                     | Nhóm 4. Trung lập   |
# | BARRIER                     | Nhóm 5. Rào cản     |
# | AT_RISK                     | Nhóm 6. Rủi ro      |
# | CRITICAL                    | Nhóm 7. Khủng hoảng |
# | UNCLASSIFIED                | Không đủ dữ liệu    |

# 1 = EXCELLENT
# 2 = ENGAGED
# 3 = STABLE
# 4 = NEUTRAL
# 5 = BARRIER
# 6 = AT_RISK
# 7 = CRITICAL
# 99 = UNCLASSIFIED

# | Cột                           | Ý nghĩa                                                         |
# | ----------------------------- | --------------------------------------------------------------- |
# | respondent_uuid               | Khách hàng                                                      |
# | response_id                   | Response                                                        |
# | survey_id                     | Survey                                                          |
# | collected_date                | Ngày khảo sát                                                   |
# | cso_ticket_id                 | Ticket                                                          |
# | customer_nps_score            | Điểm NPS                                                        |
# | customer_csat_score           | Điểm CSAT                                                       |
# | customer_ces_score            | Điểm CES                                                        |
# | has_complete_cx_score         | Đủ dữ liệu hay chưa                                             |
# | customer_experience_segment   | EXCELLENT, ENGAGED, STABLE, BARRIER, AT_RISK, CRITICAL, NEUTRAL |
# | customer_experience_sentiment | POSITIVE / NEUTRAL / NEGATIVE                                   |
# | customer_experience_score     | -6 → +6                                                         |

# | Cột                           | Ý nghĩa                                           |
# | ----------------------------- | ------------------------------------------------- |
# | `kpi_group`                   | NPS / CSAT / CES                                  |
# | `kpi_type`                    | CSAT_RELATIONAL, CSAT_TRANSACTIONAL...            |
# | `kpi_sub_group`               | PROMOTER, PASSIVE, VERY_SATISFIED...              |
# | `kpi_score`                   | Điểm gốc (0–10, 1–5, 1–7)                         |
# | `kpi_score_normalized`        | Quy đổi về thang 0–100 để so sánh giữa các KPI    |
# | `customer_experience_score`   | Điểm tổng hợp (-6 đến 6 hoặc thang điểm bạn chọn) |
# | `customer_experience_segment` | EXCELLENT, CRITICAL...                            |
# | `customer_experience_rank`    | 1–7                                               |
# | `customer_experience_label`   | Tên hiển thị tiếng Việt                           |
# | `has_complete_cx_score`       | Đủ NPS + CSAT + CES hay chưa                      |
