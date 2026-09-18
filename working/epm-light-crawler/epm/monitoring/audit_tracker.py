import os
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from epm.monitoring.metrics import CrawlRunMetrics, CrawlStatus
from epm.storage.state_store import StateStore
from epm.storage.minio_sink import MinIOSink

logger = logging.getLogger(__name__)


class AuditTracker:
    """Enterprise Audit Tracking & Observability Engine.
    1. Records execution metadata for every crawl into local SQLite table 'crawler_audit_runs'.
    2. Packages daily audit logs into JSONL and mirrors them to MinIO at:
       lakehouse/audit/epm_crawler_audit_YYYY-MM-DD.jsonl
    3. Provides operational query tools for health checks and status reporting.
    """

    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        sink: Optional[MinIOSink] = None,
        local_audit_dir: str = "./data/audit",
    ):
        self.state_store = state_store or StateStore()
        self.sink = sink or MinIOSink(state_store=self.state_store)
        self.local_audit_dir = os.path.abspath(local_audit_dir)
        os.makedirs(self.local_audit_dir, exist_ok=True)

    def log_run(self, metrics: CrawlRunMetrics):
        """Persists a crawl execution's metrics into SQLite and appends to local daily audit log."""
        metrics_dict = metrics.to_dict()
        self.state_store.record_audit_run(metrics_dict)

        # Append to local daily audit file
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_log_file = os.path.join(self.local_audit_dir, f"audit_{date_str}.jsonl")

        with open(daily_log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(metrics_dict, ensure_ascii=False) + "\n")

        logger.info(json.dumps({
            "event": "audit_logged",
            "batch_id": metrics.batch_id,
            "endpoint": metrics.endpoint_name,
            "status": metrics.status,
            "records": metrics.records_extracted,
            "requests": metrics.requests_sent,
            "retries": metrics.retry_count,
            "duration_sec": metrics.duration_seconds,
        }))

    def sync_daily_audit_to_minio(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """Mirrors the daily JSONL audit log to MinIO under:
        lakehouse/audit/epm_crawler_audit_YYYY-MM-DD.jsonl
        """
        date_str = target_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_log_file = os.path.join(self.local_audit_dir, f"audit_{date_str}.jsonl")

        if not os.path.exists(daily_log_file):
            logger.info(f"No audit logs found for date {date_str} to sync.")
            return {"status": "NO_FILE", "date": date_str}

        with open(daily_log_file, "rb") as f:
            data = f.read()

        s3_key = f"audit/epm_crawler_audit_{date_str}.jsonl"
        try:
            if not self.sink.client.bucket_exists(self.sink.bucket_name):
                self.sink.client.make_bucket(self.sink.bucket_name)

            self.sink.client.put_object(
                bucket_name=self.sink.bucket_name,
                object_name=s3_key,
                data=io.BytesIO(data),
                length=len(data),
                content_type="application/x-ndjson",
            )
            s3_uri = f"s3://{self.sink.bucket_name}/{s3_key}"
            logger.info(f"Successfully synced daily audit log to MinIO: {s3_uri}")
            return {
                "status": "SYNCED",
                "date": date_str,
                "s3_uri": s3_uri,
                "bytes": len(data),
            }
        except Exception as e:
            logger.warning(f"Failed to sync daily audit log to MinIO ({e}). Local copy preserved.")
            return {
                "status": "FAILED",
                "date": date_str,
                "error": str(e),
                "local_path": daily_log_file,
            }

    def get_recent_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieves recent crawl runs for telemetry/monitoring dashboards."""
        return self.state_store.list_recent_audit_runs(limit=limit)

    def get_system_health(self) -> Dict[str, Any]:
        """Returns high-level health report: daily quota, pending fallback files, MinIO status."""
        quota_info = self.state_store.get_daily_requests()
        pending_buffers = self.state_store.list_pending_buffers()
        recent_runs = self.state_store.list_recent_audit_runs(limit=5)
        minio_healthy = self.sink.is_connected()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "minio_reachable": minio_healthy,
            "daily_requests": quota_info["request_count"],
            "daily_limit": quota_info["max_limit"],
            "pending_fallback_buffers": len(pending_buffers),
            "recent_runs": recent_runs,
        }
