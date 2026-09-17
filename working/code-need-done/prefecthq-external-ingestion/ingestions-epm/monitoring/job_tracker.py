import time
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class JobTracker:
    def __init__(self, job_name, endpoint_name):
        self.job_name = job_name
        self.endpoint_name = endpoint_name
        self._start_time = None
        self._end_time = None

        self._total_pages = 0
        self._total_records_extracted = 0
        self._extraction_errors = 0

        self._total_records_written = 0
        self._valid_records = 0
        self._dlq_records = 0
        self._quality_report = None

        self._client_stats = None


    def start(self):
        self._start_time = time.monotonic()
        self._start_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(json.dumps({
            "event": "job_started", 
            "job_name": self.job_name,
            "endpoint": self.endpoint_name,
            "timestamp": self._start_timestamp
        }))


    def track_extraction(self, batch):
        self._total_pages += 1
        self._total_records_extracted += batch.record_count

        logger.info(json.dumps({
            "event": "extraction_progress",
            "endpoint": self.endpoint_name,
            "page": batch.page_number,
            "records_this_page": batch.record_count,
            "total_records": self._total_records_extracted,
            "total_pages": self._total_pages
        }))


    def track_transform(self, record_count):
        self._total_records_written += record_count

    def track_quality(self, quality_report):
        self._quality_report = quality_report
        self._valid_records = quality_report.valid_records
        self._dlq_records = quality_report.dlq_records

    def set_client_stats(self, client_stats):
        self._client_stats = client_stats

    def finish(self, status="SUCCESS"):
        self._end_time = time.monotonic()
        end_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        duration_seconds = (
            self._end_time - self._start_time if self._start_time is not None else 0
        )

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
            "client_stats": self._client_stats
        }

        logger.info(json.dumps({
            "event": "job_completed",
            **summary
        }))
        return summary

    def _to_daily_report_markdown(self, summary=None):
        if summary is None:
            summary = self.finish()

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        