%livy.pyspark

target_table = "bi_gold.cx_survey_kpi_metrics"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/cx_survey_kpi_metrics"

# hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# spark.sql(f"DROP TABLE IF EXISTS {target_table}")

sql_query = """
WITH q_dim AS (
  SELECT
    survey_id,
    question_id,
    MAX(CAST(question_clean AS STRING)) AS question_clean
  FROM bi_silver.cx_sur_question_answer_choices
  GROUP BY survey_id, question_id
),

base AS (
  SELECT
    fa.collected_date,
    CAST(fa.product_category AS STRING)            AS product_category,
    CAST(fa.customer_interaction_type AS STRING)   AS customer_interaction_type,
    CAST(fa.question_type AS STRING)               AS question_type,
    CAST(fa.kpi_type AS STRING)                    AS kpi_type,
    CAST(fa.answer_number AS DOUBLE)               AS score,
    CAST(fa.answer_tag AS STRING)                  AS answer_tag,
    LOWER(COALESCE(q.question_clean, ''))          AS question_clean_l
  FROM bi_silver.cx_sur_question_response fa
  LEFT JOIN q_dim q
    ON CAST(fa.survey_id AS STRING)   = CAST(q.survey_id AS STRING)
   AND CAST(fa.question_id AS STRING) = CAST(q.question_id AS STRING)
  WHERE fa.collected_date IS NOT NULL
),

rating_only AS (
  SELECT *
  FROM base
  WHERE (
        question_type = 'rating'
        OR lower(coalesce(kpi_type,'')) RLIKE 'nps'
        OR lower(coalesce(question_type,'')) = 'nps'
    )
    AND score IS NOT NULL
),

classified AS (
  SELECT
    collected_date,
    product_category,
    customer_interaction_type,
    answer_tag,
    score,
    CASE
      WHEN kpi_type = 'CSAT điểm chạm' THEN 'CSAT_TRANSACTIONAL'
      WHEN kpi_type = 'CSAT SPDV'      THEN 'CSAT_RELATIONAL'
      WHEN lower(kpi_type) = 'ces'     THEN 'CES'
      WHEN lower(kpi_type) RLIKE 'nps' THEN 'NPS'

      WHEN (kpi_type IS NULL OR kpi_type = 'No Data')
        AND question_clean_l RLIKE 'giới thiệu|recommend|khuyến nghị|promoter|nps'
        THEN 'NPS'

      WHEN (kpi_type IS NULL OR kpi_type = 'No Data')
        AND question_clean_l RLIKE 'nỗ lực|effort|dễ dàng|khó khăn|ces'
        THEN 'CES'

      WHEN (kpi_type IS NULL OR kpi_type = 'No Data')
        AND question_clean_l RLIKE 'hài lòng|satisfaction|csat'
        THEN CASE
          WHEN lower(coalesce(customer_interaction_type,'')) RLIKE
               'ticket|cso|call|hotline|support|cskh|khiếu nại|incident'
            THEN 'CSAT_TRANSACTIONAL'
          ELSE 'CSAT_RELATIONAL'
        END
      ELSE NULL
    END AS kpi_group
  FROM rating_only
),

kpi_only AS (
  SELECT *
  FROM classified
  WHERE kpi_group IS NOT NULL
    AND (
      kpi_group <> 'NPS'
      OR (score BETWEEN 0 AND 10)
      OR (score BETWEEN 1 AND 11)
    )
),

periodized AS (
  SELECT
    'month' AS period_type,
    date_trunc('month', collected_date)                 AS period_start,
    CAST(date_trunc('month', collected_date) AS DATE)   AS period_start_date,
    product_category, customer_interaction_type, kpi_group,
    answer_tag, score
  FROM kpi_only

  UNION ALL
  SELECT
    'quarter',
    date_trunc('quarter', collected_date),
    CAST(date_trunc('quarter', collected_date) AS DATE),
    product_category, customer_interaction_type, kpi_group,
    answer_tag, score
  FROM kpi_only

  UNION ALL
  SELECT
    'year',
    date_trunc('year', collected_date),
    CAST(date_trunc('year', collected_date) AS DATE),
    product_category, customer_interaction_type, kpi_group,
    answer_tag, score
  FROM kpi_only
)

SELECT
  period_type,
  CAST(period_start AS TIMESTAMP) AS period_start,
  period_start_date,
  product_category,
  customer_touchpoint as customer_interaction_type,

  SUM(CASE WHEN kpi_group = 'CSAT_RELATIONAL' THEN score ELSE 0 END) AS csat_rel_sum_score,
  SUM(CASE WHEN kpi_group = 'CSAT_RELATIONAL' THEN 1 ELSE 0 END)     AS csat_rel_cnt,

  SUM(CASE WHEN kpi_group = 'CSAT_TRANSACTIONAL' THEN score ELSE 0 END) AS csat_tran_sum_score,
  SUM(CASE WHEN kpi_group = 'CSAT_TRANSACTIONAL' THEN 1 ELSE 0 END)     AS csat_tran_cnt,

  SUM(CASE WHEN kpi_group = 'CES' THEN score ELSE 0 END) AS ces_sum_score,
  SUM(CASE WHEN kpi_group = 'CES' THEN 1 ELSE 0 END)     AS ces_cnt,

  SUM(CASE WHEN kpi_group = 'NPS'
            AND (CASE WHEN score BETWEEN 1 AND 11 THEN score - 1 ELSE score END) BETWEEN 9 AND 10
           THEN 1 ELSE 0 END) AS nps_promoters_cnt,

  SUM(CASE WHEN kpi_group = 'NPS'
            AND (CASE WHEN score BETWEEN 1 AND 11 THEN score - 1 ELSE score END) BETWEEN 0 AND 6
           THEN 1 ELSE 0 END) AS nps_detractors_cnt,

  SUM(CASE WHEN kpi_group = 'NPS'
            AND (CASE WHEN score BETWEEN 1 AND 11 THEN score - 1 ELSE score END) BETWEEN 0 AND 10
           THEN 1 ELSE 0 END) AS nps_total_cnt

FROM periodized
GROUP BY
  period_type,
  period_start,
  period_start_date,
  product_category,
  customer_interaction_type
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

# spark.sql(f"SELECT count(*) AS total_gold FROM {target_table}").show()
# spark.sql(f"""
# SELECT period_type, CAST(period_start as TIMESTAMP) as period_start,
#       sum(csat_rel_cnt) rel_cnt,
#       sum(csat_tran_cnt) tran_cnt,
#       sum(ces_cnt) ces_cnt,
#       sum(nps_total_cnt) nps_cnt
# FROM {target_table}
# GROUP BY period_type, period_start
# ORDER BY period_type, period_start DESC
# """).show(60, False)
