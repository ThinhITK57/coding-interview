import time
import json
import logging

from collections import defaultdict

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self):
        self._request_durations = []
        self._status_counts = defaultdict(int)
        self._start_time = time.monotonic()


    def record_request(self, duration_ms, status_code=None):
        self._request_durations.append(duration_ms)
        if status_code is not None:
            self._status_counts[status_code] += 1

    def get_summary(self):
        if not self._request_durations:
            return {
                "total_requests": 0,
                "avg_duration_ms": 0,
                "p95_duration_ms": 0,
                "throughput_rps": 0,
                "status_distribution": {}
            }

        durations = sorted(self._request_durations)
        total = len(durations)
        elapsed = time.monotonic() - self._start_time

        p95_index = int(total * 0.95)
        p95 = durations[min(p95_index, total - 1)]

        return {
            "total_requests": total,
            "avg_durations_ms": round(sum(durations) / total, 1),
            "p95_durations": round(p95, 1),
            "throughput_rps": round(total / elapsed, 2) if elapsed > 0 else 0,
            "status_distribution": dict(self._status_counts)
        }