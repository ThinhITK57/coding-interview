# %livy.pyspark

from pyspark.sql import functions as F
from datetime import datetime
target_table =  "bi_silver_snapshot.cx_cso_support_ticket_ttr_snapshot"
target_path = "s3a://vcs-silver/bi-silver-snapshot/cx_cso_support_ticket_ttr_snapshot"

# ---------------------------------------------------------------------
# Resolve snapshot configuration
# - If `snapshot_date` is provided by Prefect (YYYY-MM-DD), use it.
# - Otherwise, default to today's date.
# ---------------------------------------------------------------------
snapshot_date = params.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))

snapshot_dt = datetime.strptime(snapshot_date, "%Y-%m-%d")
snapshot_suffix = snapshot_dt.strftime("%Y%m")

print("Snapshot month : {}".format(snapshot_dt.strftime("%Y-%m")))
print("Target table   : {}".format(target_table))

# ---------------------------------------------------------------------
# Refresh Spark metadata to ensure the latest source data is used.
# ---------------------------------------------------------------------
spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.cx_cso_support_tickets")


# ---------------------------------------------------------------------
# Extract records for the snapshot month.
# ---------------------------------------------------------------------
year = snapshot_dt.year
month = snapshot_dt.month

df = spark.sql("""
SELECT
    ticket_id,
    due_by,
    ttr_overdue,
    issues_type,
    CAST( '%s' as date ) AS snapshot_date,
    current_timestamp() AS audit_snapshot_ts
FROM bi_silver.cx_cso_support_tickets
WHERE YEAR(due_by) = %s
  AND MONTH(due_by) = %s
""" % (snapshot_date, year, month))

# ---------------------------------------------------------------------
# Persist snapshot as a dedicated monthly table.
# ---------------------------------------------------------------------
(
    df
        .repartition(1)
        .write
        .mode("append")
        .format("parquet")
        .option("path", target_path)
        .saveAsTable(target_table)
)

# Refresh metadata for downstream consumers.
spark.catalog.refreshTable(target_table)

print("Snapshot table created: {}".format(target_table))