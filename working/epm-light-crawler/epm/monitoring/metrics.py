from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class ExtractionMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


class CrawlStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


@dataclass
class RateLimitConfig:
    requests_per_minute: int = 60
    requests_per_day: int = 1000


@dataclass
class RetryConfig:
    max_attempts: int = 5
    backoff_factor: float = 2.0
    max_backoff_seconds: float = 60.0
    retry_status_codes: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])


@dataclass
class PaginationConfig:
    type: str = "offset"
    location: str = "body"
    page_size: int = 250
    max_pages: int = 10000
    offset_param: str = "from"
    limit_param: str = "limit"


@dataclass
class EndpointConfig:
    name: str
    path: str
    method: str = "POST"
    headers: Dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})
    auth_type: str = "api_key"
    auth_header: str = "Authorization"
    auth_env_name: str = "PROJECT_API_KEY"
    auth_prefix: str = "ApiKey"
    timeout_seconds: int = 30
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    body: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Batch:
    records: List[Dict[str, Any]]
    batch_id: str
    page_number: int
    record_count: int
    endpoint_name: str
    window_start: Optional[str] = None
    window_end: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "endpoint_name": self.endpoint_name,
            "page_number": self.page_number,
            "record_count": self.record_count,
            "window_start": self.window_start,
            "window_end": self.window_end,
        }


@dataclass
class CrawlRunMetrics:
    batch_id: str
    endpoint_name: str
    mode: str
    start_time: str
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    records_extracted: int = 0
    requests_sent: int = 0
    retry_count: int = 0
    watermark_start: Optional[str] = None
    watermark_end: Optional[str] = None
    status: str = CrawlStatus.SUCCESS.value
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "endpoint_name": self.endpoint_name,
            "mode": self.mode,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "records_extracted": self.records_extracted,
            "requests_sent": self.requests_sent,
            "retry_count": self.retry_count,
            "watermark_start": self.watermark_start,
            "watermark_end": self.watermark_end,
            "status": self.status,
            "error_message": self.error_message,
        }
