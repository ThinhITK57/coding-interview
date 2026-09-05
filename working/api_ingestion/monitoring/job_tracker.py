import time
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class JobTracker:
    """
    Tracks job execution metrics for daily progress monitoring.

    Deep module design:
        Interface (small): start(), track_extraction(batch), track_transform(count),
            track_quality(report), finish() -> summary dict
        Implementation (deep): Aggregates timing, record counts, request metrics,
        error counts, and rate limit usage across the entire job lifecycle.
        Generates structured logs and daily summary reports.
    """

    def __init__(self, job_name, endpoint_name):
        """Initialize job tracker.

        Args:
            job_name: Name of the ingestion job.
            endpoint_name: API endpoint being processed.
        """
        self.job_name = job_name
        self.endpoint_name = endpoint_name
        self._start_time = None
        self._end_time = None

        # Extraction metrics
        self._total_pages = 0
        self._total_records_extracted = 0
        self._extraction_errors = 0

        # Transform metrics
        self._total_records_written = 0

        # Quality metrics
        self._valid_records = 0
        self._dlq_records = 0
        self._quality_report = None

        # Client metrics (populated from ResilientHTTPClient.get_stats())
        self._client_stats = None

    def start(self):
        """Mark job start."""
        self._start_time = time.monotonic()
        self._start_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(json.dumps({
            "event": "job_started",
            "job_name": self.job_name,
            "endpoint": self.endpoint_name,
            "timestamp": self._start_timestamp,
        }))

    def track_extraction(self, batch):
        """Track a successfully extracted batch.

        Args:
            batch: Batch object from Extractor.
        """
        self._total_pages += 1
        self._total_records_extracted += batch.record_count

        logger.info(json.dumps({
            "event": "extraction_progress",
            "endpoint": self.endpoint_name,
            "page": batch.page_number,
            "records_this_page": batch.record_count,
            "total_records": self._total_records_extracted,
            "total_pages": self._total_pages,
        }))

    def track_transform(self, record_count):
        """Track records written after transformation."""
        self._total_records_written += record_count

    def track_quality(self, quality_report):
        """Track quality check results.

        Args:
            quality_report: QualityReport instance.
        """
        self._quality_report = quality_report
        self._valid_records = quality_report.valid_records
        self._dlq_records = quality_report.dlq_records

    def set_client_stats(self, client_stats):
        """Store client metrics from ResilientHTTPClient."""
        self._client_stats = client_stats

    def finish(self, status="SUCCESS"):
        """Mark job complete and return summary.

        Args:
            status: Job completion status (SUCCESS, FAILED, PARTIAL).

        Returns:
            dict: Complete job summary with all metrics.
        """
        self._end_time = time.monotonic()
        end_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        duration_seconds = (
            self._end_time - self._start_time
            if self._start_time is not None
            else 0
        )

        # Format duration as human-readable
        minutes = int(duration_seconds // 60)
        seconds = int(duration_seconds % 60)
        duration_str = f"{minutes}m {seconds}s"

        summary = {
            "job_name": self.job_name,
            "endpoint": self.endpoint_name,
            "status": status,
            "start_time": self._start_timestamp,
            "end_time": end_timestamp,
            "duration_seconds": round(duration_seconds, 1),
            "duration_human": duration_str,
            "total_pages": self._total_pages,
            "total_records_extracted": self._total_records_extracted,
            "total_records_written": self._total_records_written,
            "valid_records": self._valid_records,
            "dlq_records": self._dlq_records,
            "client_stats": self._client_stats,
        }

        logger.info(json.dumps({
            "event": "job_completed",
            **summary,
        }))

        return summary

    def to_daily_report_markdown(self, summary=None):
        """Generate daily ingestion report as markdown for Prefect Artifact.

        Args:
            summary: Job summary dict (from finish()). Uses current state if None.

        Returns:
            str: Formatted markdown report.
        """
        if summary is None:
            summary = self.finish()

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        status_emoji = "\u2705" if summary["status"] == "SUCCESS" else "\u274c"

        lines = []
        lines.append(f"## \U0001f4ca Daily Ingestion Report \u2014 {date_str}")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|:---|:---|")
        lines.append(f"| **Endpoint** | `{summary['endpoint']}` |")
        lines.append(f"| **Status** | {status_emoji} {summary['status']} |")
        lines.append(f"| **Start Time** | {summary['start_time']} |")
        lines.append(f"| **End Time** | {summary['end_time']} |")
        lines.append(f"| **Duration** | {summary['duration_human']} |")
        lines.append(f"| **Total Pages** | {summary['total_pages']:,} |")
        lines.append(f"| **Total Records Extracted** | {summary['total_records_extracted']:,} |")
        lines.append(f"| **Total Records Written** | {summary['total_records_written']:,} |")

        if summary.get("valid_records") or summary.get("dlq_records"):
            lines.append(f"| **Valid Records** | {summary['valid_records']:,} |")
            lines.append(f"| **DLQ Records** | {summary['dlq_records']:,} |")

        # Client stats
        client = summary.get("client_stats") or {}
        if client:
            total_requests = client.get("total_requests", 0)
            total_errors = client.get("total_errors", 0)
            lines.append(f"| **Total HTTP Requests** | {total_requests:,} |")
            lines.append(f"| **Total Errors** | {total_errors:,} |")

            rl = client.get("rate_limiter", {})
            if rl.get("daily_limit"):
                daily_used = rl.get("daily_used", 0)
                daily_limit = rl.get("daily_limit", 0)
                pct = (daily_used / daily_limit * 100) if daily_limit > 0 else 0
                lines.append(f"| **Daily API Quota Used** | {daily_used:,} / {daily_limit:,} ({pct:.1f}%) |")

        lines.append("")

        # Append quality report if available
        if self._quality_report is not None:
            lines.append(self._quality_report.to_markdown())

        return "\n".join(lines)
