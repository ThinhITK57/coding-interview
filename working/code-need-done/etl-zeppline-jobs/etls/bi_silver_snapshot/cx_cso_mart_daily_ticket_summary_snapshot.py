%livy.pyspark

from datetime import datetime

target_table_tmp = "bi_silver_snapshot.cx_cso_mart_daily_ticket_summary_snapshot"
target_path_tmp = "/opt/datasets/crawlers/vcs_silver/bi_silver_snapshot/data/cx_cso_mart_daily_ticket_summary_snapshot"


# ---------------------------------------------------------------------
# Resolve snapshot configuration
# - If `snapshot_date` is provided by Prefect (YYYY-MM-DD), use it.
# - Otherwise, default to today's date.
# ---------------------------------------------------------------------
snapshot_date = globals().get(
    "snapshot_date",
    datetime.now().strftime("%Y-%m-%d")
)

snapshot_dt = datetime.strptime(snapshot_date, "%Y-%m-%d")
snapshot_suffix = snapshot_dt.strftime("%Y%m")

target_table = target_table_tmp.format(snapshot_suffix)
target_path = target_path_tmp.format(snapshot_suffix)

print ("Snapshot month : {}".format(snapshot_dt.strftime("%Y-%m")))
print ("Target table   : {}".format(target_table))

# ---------------------------------------------------------------------
# Refresh Spark metadata to ensure the latest source data is used.
# ---------------------------------------------------------------------
spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.cx_cso_mart_daily_ticket_summary")
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# Extract records for the snapshot month.
# ---------------------------------------------------------------------
year = snapshot_dt.year
month = snapshot_dt.month

cx_cso_mart_daily_ticket_summary_snapshot = spark.sql("""
SELECT *,
    CAST( '%s' as date ) AS snapshot_date,
    current_timestamp() AS audit_snapshot_ts
FROM bi_silver.cx_cso_mart_daily_ticket_summary
WHERE YEAR(report_date) = %s
  AND MONTH(report_date) = %s
""" % (snapshot_date, year, month))

# ---------------------------------------------------------------------
# Persist snapshot as a dedicated monthly table.
# ---------------------------------------------------------------------
(
    cx_cso_mart_daily_ticket_summary_snapshot
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