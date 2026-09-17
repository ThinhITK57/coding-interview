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
    """Raised when API rate limit quota is exhausted"""
    pass

class CircuitBreakerOpenError(APIError):
    """Raised when Circuit breaker is in OPEN state and rejecting requests"""
    pass

class PaginationError(APIError):
    """Raised when pagination encounters an unrecover error (infinitive loop)"""
    pass

class CheckpointError(APIError):
    """Raised when Checkpoint read/write operations fail."""
    pass