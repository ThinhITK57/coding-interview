# %livy.pyspark

# =============================================================================
# Purpose
# =============================================================================
# Provides the latest snapshot records for downstream consumption.
#
# Since snapshot tables are append-only, duplicate versions of the same record
# may exist. This view returns only the most recent version of each record.
# =============================================================================

spark.sql("""

CREATE OR REPLACE VIEW bi_silver.cx_cso_mart_daily_ticket_metrics AS

WITH latest_snapshot AS (

    SELECT
        report_date,
        sla_first_resp_success_cnt,
        sla_first_resp_total_cnt,
        sla_first_resp_ratio,
        csat_positive_cnt,
        csat_response_cnt,
        csat_positive_ratio,
        efficient_resolve_success_cnt,
        efficient_resolve_total_cnt,
        efficient_resolve_ratio,
        resolved_within_sla_cnt,
        resolution_sla_total_cnt,
        sla_ttr_ratio,
        ttr_total_days,
        resolved_ticket_cnt,
        ttr_avg_days,
        ROW_NUMBER() OVER (
            PARTITION BY report_date
            ORDER BY audit_snapshot_ts DESC
        ) AS rn
    FROM bi_silver_snapshot.cx_cso_mart_daily_ticket_summary_snapshot

)

SELECT
    report_date,
    sla_first_resp_success_cnt,
    sla_first_resp_total_cnt,
    sla_first_resp_ratio,
    csat_positive_cnt,
    csat_response_cnt,
    csat_positive_ratio,
    efficient_resolve_success_cnt,
    efficient_resolve_total_cnt,
    efficient_resolve_ratio,
    resolved_within_sla_cnt,
    resolution_sla_total_cnt,
    sla_ttr_ratio,
    ttr_total_days,
    resolved_ticket_cnt,
    ttr_avg_days
FROM latest_snapshot
WHERE rn = 1
""")