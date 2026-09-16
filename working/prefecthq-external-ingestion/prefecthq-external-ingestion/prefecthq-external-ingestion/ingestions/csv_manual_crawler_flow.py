"""
MinIO CSV-Manual Ingestion Flow
------------------------------
Reads Excel (.xlsx) and CSV files from a MinIO input bucket, normalises columns
using per-table mapping files, converts to Parquet, and uploads to a MinIO
output bucket.

Supported resource shapes
─────────────────────────
1. Single file  (object_key: str)
   Simple case — one file, one sheet.

2. Multi-source (sources: list[dict])
   When a table is built from several files and/or sheets they are all read,
   concatenated, and uploaded in a single Parquet write.  Each source entry
   accepts the same per-file keys as a single-file descriptor
   (object_key, file_type, sheet_name, header_row, drop_rows, post_process,
    add_columns).

Directory layout expected:
  resources/
    mappings/
      finance_raw/
        actual_cost.json
        cost_items.json
        cost_plan.json
      hr_raw/
        hr_employee_onboard.json
        hr_employee_resigned.json
        hr_employee_headcount.json
      cx_cso_raw/
        cx_dim_journey.json
        dim_question_customer_journey.json
        dim_surveys_customer_journey.json
"""

from prefect import flow, get_run_logger

from common.config import *
from common.http_util import *
from common.crawler_util import *
from common.ambari_util import *
from common.flat.util import *
from common.flat.reader import *
from common.flat.flat_resource import RESOURCES
from common.flat.task import read_and_normalize, upload_data

import sys

sys.path.insert(0, "/opt/prefect/flows/ingestions")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------
@flow(name="MinIO CSV-Manual Ingestion")
def run_ingestion_daily():
    run_ingestion(RESOURCES)


@flow(name="MinIO CSV-Manual Ingestion")
def run_ingestion(resources: list):
    """
    For each resource descriptor:
      1. read_and_normalize  -- fetch, normalise, concat sources, post_process
      2. upload_data         -- schema, Parquet, MinIO upload, SQL stub

    Resource descriptor keys
    ────────────────────────
    Required (single-file shape):
      resource_name : str
      object_key    : str

    Required (multi-source shape):
      resource_name : str
      sources       : list[dict]  -- each entry has its own object_key + options

    Common optional keys:
      domain        : str         -- default "cx_cso_raw"
      file_type     : str         -- "excel" | "csv" | "csv_folder"  (default "excel")
      sheet_name    : int | str   -- default 0
      header_row    : int         -- default 0
      drop_rows     : int         -- default 1
      crawl_mode    : str         -- "static" | "modified_and_new"  (default "static")
      enable_state  : bool        -- default False
      add_columns   : dict        -- static columns to inject per source
      post_process  : callable    -- top-level df -> df applied after all sources merged
    """
    logger = get_run_logger()

    for r in resources:
        name = r.get("resource_name", "<unknown>")
        try:
            records = read_and_normalize(r)
            upload_data(
                resource_name=name,
                records=records,
                domain=r.get("domain"),
                crawl_mode=r.get("crawl_mode", "static"),
                enable_state=r.get("enable_state", False),
            )
        except Exception as exc:
            logger.error(f"[{name}] Failed: {exc}", exc_info=True)


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from prefect import serve

    d1 = run_ingestion.to_deployment(
        name="[Adhoc] CSV-Manual Ingestion",
        tags=["production", "MinIO", "Excel", "CSV"],
    )

    d2 = run_ingestion_daily.to_deployment(
        name="[Daily] CSV-Manual Ingestion",
        cron="0 2 * * *",
        tags=["production", "MinIO", "Excel", "CSV"],
    )
    serve(d1, d2)
