import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# Load .env or .env.epm
env_epm = os.path.join(os.path.dirname(__file__), "..", ".env.epm")
if os.path.exists(env_epm):
    load_dotenv(env_epm)
else:
    load_dotenv()

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from prefect import flow

from epm.monitoring.metrics import CrawlRunMetrics, CrawlStatus, ExtractionMode
from epm.config.loader import ConfigLoader
from epm.storage.state_store import StateStore, DailyQuotaExceededError
from epm.storage.minio_sink import MinIOSink
from epm.ingestion.extractor import LightweightExtractor
from epm.backfill.backfill_worker import BackgroundBackfillWorker
from epm.monitoring.audit_tracker import AuditTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("epm.prefect_flow")


# ==============================================================================
# FLOWS CHÍNH
# ==============================================================================
@flow(name="[EPM] Crawl Endpoint Pipeline", log_prints=True)
def crawl_epm_endpoint(
    endpoint_name: str,
    mode: str = "incremental",
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    batch_size: Optional[int] = None,
    max_pages: Optional[int] = None,
    config_path: str = "config.json",
) -> Dict[str, Any]:
    """Prefect flow for single-endpoint extraction, compression, sinking, and audit logging."""
    start_dt = datetime.now(timezone.utc)
    batch_id = f"{start_dt.strftime('%Y%m%d_%H%M%S')}_{endpoint_name}"

    # Load configuration
    loader = ConfigLoader(config_path=config_path)
    endpoints = loader.load()
    if endpoint_name not in endpoints:
        raise ValueError(f"Endpoint '{endpoint_name}' not found in configuration. Available: {list(endpoints.keys())}")

    endpoint_config = endpoints[endpoint_name]
    state_store = StateStore()
    sink = MinIOSink(state_store=state_store)
    audit = AuditTracker(state_store=state_store, sink=sink)
    extractor = LightweightExtractor(
        endpoint_config=endpoint_config,
        state_store=state_store,
    )

    total_records = 0
    sink_results = []
    status = CrawlStatus.SUCCESS.value
    error_msg = None

    t0 = time.monotonic()
    try:
        print(f"Starting crawl for endpoint: {endpoint_name} [mode={mode}, batch_id={batch_id}]")
        for batch in extractor.extract_batches(
            mode=mode,
            window_start=window_start,
            window_end=window_end,
            batch_size=batch_size,
            max_pages=max_pages,
            batch_id=batch_id,
        ):
            res = sink.write_batch(batch)
            sink_results.append(res)
            total_records += batch.record_count
            print(f"[{endpoint_name}] Page {batch.page_number} processed: {batch.record_count} records -> {res['storage']}")

    except DailyQuotaExceededError as qe:
        status = CrawlStatus.FAILED.value
        error_msg = f"Quota Exceeded: {qe}"
        print(f"CRITICAL: {error_msg}")
        raise
    except Exception as e:
        status = CrawlStatus.FAILED.value
        error_msg = str(e)
        print(f"ERROR during extraction for {endpoint_name}: {e}")
        raise
    finally:
        duration = round(time.monotonic() - t0, 2)
        end_dt = datetime.now(timezone.utc)

        metrics = CrawlRunMetrics(
            batch_id=batch_id,
            endpoint_name=endpoint_name,
            mode=mode,
            start_time=start_dt.isoformat(),
            end_time=end_dt.isoformat(),
            duration_seconds=duration,
            records_extracted=total_records,
            requests_sent=extractor.client.requests_sent,
            retry_count=extractor.client.retry_count,
            watermark_start=window_start,
            watermark_end=window_end,
            status=status,
            error_message=error_msg,
        )
        audit.log_run(metrics)

    return {
        "batch_id": batch_id,
        "endpoint": endpoint_name,
        "mode": mode,
        "status": status,
        "records_extracted": total_records,
        "requests_sent": extractor.client.requests_sent,
        "retry_count": extractor.client.retry_count,
        "duration_seconds": duration,
        "sink_results": sink_results,
    }


@flow(name="[EPM] Master DAG Daily Pipeline", log_prints=True)
def crawl_epm_master_dag(mode: str = "incremental", config_path: str = "config.json") -> Dict[str, Any]:
    """Master DAG orchestrating all 5 EPM endpoints in dependency order:
    projects -> tasks -> targets -> objectives -> assignments
    """
    ordered_endpoints = ["projects", "tasks", "targets", "objectives", "assignments"]
    results = {}
    print(f"=== Starting Master DAG Flow [mode={mode}] ===")

    for ep in ordered_endpoints:
        print(f"--- Running step: {ep} ---")
        try:
            res = crawl_epm_endpoint(endpoint_name=ep, mode=mode, config_path=config_path)
            results[ep] = res
        except Exception as e:
            results[ep] = {"status": "FAILED", "error": str(e)}
            print(f"Step {ep} failed: {e}")

    print("=== Master DAG Flow Completed ===")
    return results


@flow(name="[EPM] Background Backfill Pipeline", log_prints=True)
def background_backfill_flow() -> Dict[str, Any]:
    """Periodic background flow to drain fallback buffer files to MinIO and sync audit logs."""
    print("=== Starting Background Backfill Flow ===")
    state_store = StateStore()
    sink = MinIOSink(state_store=state_store)
    worker = BackgroundBackfillWorker(sink=sink, state_store=state_store)
    backfill_result = worker.run_backfill()

    audit = AuditTracker(state_store=state_store, sink=sink)
    sync_result = audit.sync_daily_audit_to_minio()

    return {
        "backfill": backfill_result,
        "audit_sync": sync_result,
    }


# ==============================================================================
# CLI CHO SERVICE EPM-ADHOC
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="EPM Crawler Ad-hoc Execution")
    parser.add_argument("--endpoint", "-e", choices=["tasks", "projects", "targets", "objectives", "assignments"], help="Endpoint to crawl")
    parser.add_argument("--mode", "-m", default="incremental", choices=["incremental", "full", "backfill"], help="Crawl mode")
    parser.add_argument("--start", help="Override window start (ISO format, e.g. 2026-09-18T00:00:00Z)")
    parser.add_argument("--end", help="Override window end (ISO format)")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit max pages")
    parser.add_argument("--dag", action="store_true", help="Run master DAG across all endpoints")
    parser.add_argument("--backfill", action="store_true", help="Run background backfill pass")
    parser.add_argument("--status", action="store_true", help="Check system health and daily quota")

    args = parser.parse_args()

    if args.dag:
        res = crawl_epm_master_dag(mode=args.mode)
        print(json.dumps(res, indent=2))
    elif args.backfill:
        res = background_backfill_flow()
        print(json.dumps(res, indent=2))
    elif args.status:
        audit = AuditTracker()
        health = audit.get_system_health()
        print(json.dumps(health, indent=2))
    elif args.endpoint:
        res = crawl_epm_endpoint(
            endpoint_name=args.endpoint,
            mode=args.mode,
            window_start=args.start,
            window_end=args.end,
            max_pages=args.max_pages,
        )
        print(json.dumps(res, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
