from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class HttpMethod(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class AuthType(str, Enum):
    NONE = "none"
    BEARER = "bearer"
    BASIC = "basic"
    API_KEY = "api_key"
    SESSION = "session"
    OAUTH2 = "oauth2"


class PaginationType(str, Enum):
    NONE = "none"
    PAGE = "page"
    OFFSET = "offset"
    CURSOR = "cursor"
    LINK = "link"


@dataclass
class AuthConfig:
    type: AuthType
    credentials: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitConfig:
    requests_per_minute: Optional[int] = None
    requests_per_day: Optional[int] = None
    burst_limit: Optional[int] = None


@dataclass
class RetryConfig:
    max_attempts: int = 3
    backoff_factor: float = 1.0
    max_backoff_seconds: int = 60
    retry_status_codes: List[int] = field(
        default_factory=lambda: [408, 429, 500, 502, 503, 504]
    )


@dataclass
class PaginationConfig:
    type: PaginationType = PaginationType.NONE

    # API có thể đặt pagination trong query hoặc body
    location: str = "query"

    page_size: int = 100

    max_pages: Optional[int] = None
    max_records: Optional[int] = None

    offset_param: str = "offset"
    limit_param: str = "limit"
    page_param: str = "page"
    cursor_param: str = "cursor"


@dataclass
class EndpointConfig:
    name: str
    path: str

    method: HttpMethod = HttpMethod.GET

    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)

    body: Optional[Dict[str, Any]] = None

    timeout_seconds: int = 30

    auth: Optional[AuthConfig] = None

    rate_limit: Optional[RateLimitConfig] = None

    retry: RetryConfig = field(
        default_factory=RetryConfig
    )

    pagination: Optional[PaginationConfig] = None


@dataclass
class APIConfig:
    name: str
    version: str
    base_url: str

    endpoints: List[EndpointConfig] = field(
        default_factory=list
    )