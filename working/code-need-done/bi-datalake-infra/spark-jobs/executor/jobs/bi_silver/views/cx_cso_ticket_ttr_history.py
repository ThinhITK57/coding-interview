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

CREATE OR REPLACE VIEW bi_silver.cx_cso_ticket_ttr_history  AS

WITH latest_snapshot AS (

    SELECT
        ticket_id,
        due_by as report_date,
        ttr_overdue,
        issues_type,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_id, due_by
            ORDER BY audit_snapshot_ts DESC
        ) AS rn
    FROM bi_silver_snapshot.cx_cso_support_ticket_ttr_snapshot

)

SELECT
    ticket_id,
    report_date,
    ttr_overdue,
    issues_type
FROM latest_snapshot
WHERE rn = 1
""")