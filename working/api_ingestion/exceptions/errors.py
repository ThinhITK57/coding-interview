class APIError(Exception):
    """Base exception for API ingestion framework."""
    pass


class APIHTTPError(APIError):
    """HTTP error returned by API server."""

    def __init__(
        self,
        status_code: int,
        message: str,
        response_body=None,
    ):
        self.status_code = status_code
        self.response_body = response_body

        super().__init__(message)


class APINetworkError(APIError):
    """Network-level error when calling API."""
    pass


class RateLimitExceededError(APIError):
    """Raised when API rate limit quota (per-minute or daily) is exhausted."""
    pass


class CircuitBreakerOpenError(APIError):
    """Raised when circuit breaker is in OPEN state and rejecting requests."""
    pass


class PaginationError(APIError):
    """Raised when pagination encounters an unrecoverable error (infinite loop, missing cursor, etc.)."""
    pass


class CheckpointError(APIError):
    """Raised when checkpoint read/write operations fail."""
    pass