# %livy.pyspark

from pyspark.sql import functions as F

target_table = "bi_silver.cx_cso_mart_daily_ticket_summary"
target_path  = "s3a://bi-silver/cx_cso_mart_daily_ticket_summary"

# 0) Refresh metadata
spark.sql("REFRESH TABLE bi_silver.cx_cso_support_tickets")
spark.sql("REFRESH TABLE bi_silver.cx_sur_question_response")

spark.catalog.clearCache()

support_tickets_src = spark.sql("""
    SELECT
        ticket_id,

        TO_DATE(fr_due_by) AS first_response_date,
        TO_DATE(due_by) AS resolution_due_date,
        TO_DATE(closed_at) AS closed_date,

        is_overdue, -- <=> rt_overdue
        ttr_overdue,

        communication_effectiveness,
        status_name,

        (COALESCE(l1_time_actual_minutes, 0.0)
        + COALESCE(l2_time_actual_minutes, 0.0)
        + COALESCE(l3_time_actual_minutes, 0.0)
        + COALESCE(l4_time_actual_minutes, 0.0))
        / 1440.0 * 1.0
        
            AS ttr_total_days

    FROM bi_silver.cx_cso_support_tickets
    WHERE LOWER(issues_type) NOT IN ("spam", "task", "change")
""")

support_tickets_src.createOrReplaceTempView( "support_tickets_src")

survey_latest_src = spark.sql("""
    SELECT
        cso_ticket_id AS ticket_id,
        question_id,
        item_score_value,
        collected_date

    FROM (
        SELECT
            cso_ticket_id,
            question_id,
            item_score_value,
            collected_date,

            ROW_NUMBER() OVER (
                PARTITION BY
                    cso_ticket_id,
                    question_id

                ORDER BY
                    collected_date DESC
            ) AS rn

        FROM bi_silver.cx_sur_question_response

        WHERE cso_ticket_id IS NOT NULL
          AND customer_touchpoint =
              'KS hoàn tất xử lý ticket'
          AND question_type = 'rating'
    ) t

    WHERE rn = 1
""")

survey_latest_src.createOrReplaceTempView( "survey_latest_src")

survey_csat_src = spark.sql("""
    SELECT
        TO_DATE(collected_date) AS report_date,

        SUM(
            CASE
                WHEN item_score_value IN (4, 5)
                THEN 1
                ELSE 0
            END
        ) AS csat_positive_cnt,

        COUNT(item_score_value)
            AS csat_response_cnt

    FROM survey_latest_src

    WHERE collected_date IS NOT NULL

    GROUP BY
        TO_DATE(collected_date)
""")

survey_csat_src.createOrReplaceTempView( "survey_csat_src")


sla_first_response_daily = spark.sql("""
    SELECT
        first_response_date AS report_date,

        COUNT(DISTINCT CASE
            WHEN is_overdue = FALSE
            THEN ticket_id
        END) AS sla_first_resp_success_cnt,

        COUNT(DISTINCT CASE
            WHEN is_overdue IS NOT NULL
            THEN ticket_id
        END) AS sla_first_resp_total_cnt

    FROM support_tickets_src

    WHERE first_response_date IS NOT NULL

    GROUP BY
        first_response_date
""")

sla_first_response_daily.createOrReplaceTempView( "sla_first_response_daily")

efficient_resolution_daily = spark.sql("""
    SELECT
        closed_date AS report_date,

        COUNT(DISTINCT CASE
            WHEN communication_effectiveness = 'Cao'
            AND status_name IN ('Closed', 'Resolved')
            THEN ticket_id
        END) AS efficient_resolve_success_cnt,

        COUNT(DISTINCT CASE
            WHEN status_name IN ('Closed', 'Resolved')
            AND communication_effectiveness != 'Chưa xác định'
            THEN ticket_id
        END) AS efficient_resolve_total_cnt

    FROM support_tickets_src

    WHERE closed_date IS NOT NULL

    GROUP BY
        closed_date
""")

efficient_resolution_daily.createOrReplaceTempView( "efficient_resolution_daily")

sla_ttr_daily = spark.sql("""
    SELECT
        resolution_due_date AS report_date,

        COUNT(DISTINCT CASE
            WHEN ttr_overdue = 'No'
            THEN ticket_id
        END) AS resolved_within_sla_cnt,

        COUNT(DISTINCT CASE
            WHEN ttr_overdue IS NOT NULL
            THEN ticket_id
        END) AS resolution_sla_total_cnt                    

    FROM support_tickets_src

    WHERE resolution_due_date IS NOT NULL

    GROUP BY
        resolution_due_date
""")

sla_ttr_daily.createOrReplaceTempView( "sla_ttr_daily")

resolution_time_daily = spark.sql("""
    SELECT
        resolution_due_date AS report_date,
    SUM(
        CASE
            WHEN ttr_overdue != 'N/A' 
             AND ttr_total_days > 0.0
            THEN ttr_total_days
            ELSE 0.0
        END
    ) AS ttr_total_days,

    COUNT(DISTINCT CASE
        WHEN ttr_overdue != 'N/A' 
         AND ttr_total_days > 0.0
        THEN ticket_id
    END) AS resolved_ticket_cnt

    FROM support_tickets_src

    WHERE resolution_due_date IS NOT NULL

    GROUP BY
        resolution_due_date
""")

resolution_time_daily.createOrReplaceTempView("resolution_time_daily")

sla_first_response_metric = spark.sql("""
    SELECT
        report_date,

        sla_first_resp_success_cnt,
        sla_first_resp_total_cnt,

        0L AS csat_positive_cnt,
        0L AS csat_response_cnt,

        0L AS efficient_resolve_success_cnt,
        0L AS efficient_resolve_total_cnt,

        0L AS resolved_within_sla_cnt,
        0L AS resolution_sla_total_cnt,

        0.0 AS ttr_total_days,
        0L AS resolved_ticket_cnt

    FROM sla_first_response_daily
""")

sla_first_response_metric.createOrReplaceTempView("sla_first_response_metric")

csat_metric = spark.sql("""
    SELECT
        report_date,

        0L AS sla_first_resp_success_cnt,
        0L AS sla_first_resp_total_cnt,

        csat_positive_cnt,
        csat_response_cnt,

        0L AS efficient_resolve_success_cnt,
        0L AS efficient_resolve_total_cnt,

        0L AS resolved_within_sla_cnt,
        0L AS resolution_sla_total_cnt,

        0.0 AS ttr_total_days,
        0L AS resolved_ticket_cnt

    FROM survey_csat_src
""")

csat_metric.createOrReplaceTempView( "csat_metric")

efficient_resolution_metric = spark.sql("""
    SELECT
        report_date,

        0L AS sla_first_resp_success_cnt,
        0L AS sla_first_resp_total_cnt,

        0L AS csat_positive_cnt,
        0L AS csat_response_cnt,

        efficient_resolve_success_cnt,
        efficient_resolve_total_cnt,

        0L AS resolved_within_sla_cnt,
        0L AS resolution_sla_total_cnt,

        0.0 AS ttr_total_days,
        0L AS resolved_ticket_cnt

    FROM efficient_resolution_daily
""")

efficient_resolution_metric.createOrReplaceTempView("efficient_resolution_metric")

sla_ttr_metric = spark.sql("""
    SELECT
        report_date,

        0L AS sla_first_resp_success_cnt,
        0L AS sla_first_resp_total_cnt,

        0L AS csat_positive_cnt,
        0L AS csat_response_cnt,

        0L AS efficient_resolve_success_cnt,
        0L AS efficient_resolve_total_cnt,

        resolved_within_sla_cnt,
        resolution_sla_total_cnt,

        0.0 AS ttr_total_days,
        0L AS resolved_ticket_cnt

    FROM sla_ttr_daily
""")

sla_ttr_metric.createOrReplaceTempView( "sla_ttr_metric")

resolution_time_metric = spark.sql("""
    SELECT
        report_date,

        0L AS sla_first_resp_success_cnt,
        0L AS sla_first_resp_total_cnt,

        0L AS csat_positive_cnt,
        0L AS csat_response_cnt,

        0L AS efficient_resolve_success_cnt,
        0L AS efficient_resolve_total_cnt,

        0L AS resolved_within_sla_cnt,
        0L AS resolution_sla_total_cnt,

        ttr_total_days,
        resolved_ticket_cnt

    FROM resolution_time_daily
""")

resolution_time_metric.createOrReplaceTempView( "resolution_time_metric")

daily_metric_union = spark.sql("""
    SELECT * FROM sla_first_response_metric

    UNION ALL

    SELECT * FROM csat_metric

    UNION ALL

    SELECT * FROM efficient_resolution_metric

    UNION ALL

    SELECT * FROM sla_ttr_metric

    UNION ALL

    SELECT * FROM resolution_time_metric
""")

daily_metric_union.createOrReplaceTempView( "daily_metric_union")

ticket_metric_daily = spark.sql("""
    SELECT
        report_date,

        /* =================================================
           First Response SLA
           ================================================= */

        SUM(sla_first_resp_success_cnt)
            AS sla_first_resp_success_cnt,

        SUM(sla_first_resp_total_cnt)
            AS sla_first_resp_total_cnt,

        CASE
            WHEN SUM(sla_first_resp_total_cnt) = 0
            THEN 0.0

            ELSE
                SUM(sla_first_resp_success_cnt)
                * 100.0
                / SUM(sla_first_resp_total_cnt)
        END AS sla_first_resp_ratio,


        /* =================================================
           CSAT
           ================================================= */

        SUM(csat_positive_cnt)
            AS csat_positive_cnt,

        SUM(csat_response_cnt)
            AS csat_response_cnt,

        CASE
            WHEN SUM(csat_response_cnt) = 0
            THEN 0.0

            ELSE
                SUM(csat_positive_cnt)
                * 100.0
                / SUM(csat_response_cnt)
        END AS csat_positive_ratio,


        /* =================================================
           Efficient Resolution
           ================================================= */

        SUM(efficient_resolve_success_cnt)
            AS efficient_resolve_success_cnt,

        SUM(efficient_resolve_total_cnt)
            AS efficient_resolve_total_cnt,

        CASE
            WHEN SUM(efficient_resolve_total_cnt) = 0
            THEN 0.0

            ELSE
                SUM(efficient_resolve_success_cnt)
                * 100.0
                / SUM(efficient_resolve_total_cnt)
        END AS efficient_resolve_ratio,


        /* =================================================
           SLA TTR
           ================================================= */

        SUM(resolved_within_sla_cnt)
            AS resolved_within_sla_cnt,

        SUM(resolution_sla_total_cnt)
            AS resolution_sla_total_cnt,

        CASE
            WHEN SUM(resolution_sla_total_cnt) = 0
            THEN 0.0

            ELSE
                SUM(resolved_within_sla_cnt)
                * 100.0
                / SUM(resolution_sla_total_cnt)
        END AS sla_ttr_ratio,


        /* =================================================
           Resolution Time
           ================================================= */

        SUM(ttr_total_days)
            AS ttr_total_days,

        SUM(resolved_ticket_cnt)
            AS resolved_ticket_cnt,

        CASE
            WHEN SUM(resolved_ticket_cnt) = 0
            THEN 0.0

            ELSE
                SUM(ttr_total_days)
                / SUM(resolved_ticket_cnt)
        END AS ttr_avg_days

    FROM daily_metric_union

    WHERE report_date IS NOT NULL
    AND report_date <= CURRENT_DATE()
    GROUP BY
        report_date
""")


(ticket_metric_daily.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

spark.catalog.refreshTable(target_table)
print(target_table)