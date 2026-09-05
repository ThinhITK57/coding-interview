import time
import logging
import json

from client.base_client import BaseClient
from client.http_client import HTTPClient
from reliability.rate_limiter import RateLimiter
from reliability.retry import RetryExecutor
from reliability.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


class ResilientHTTPClient(BaseClient):
    """
    Production-grade HTTP client with full reliability stack.

    Deep module design:
        Interface (small):  request(method, url, ...) -> Response dict
        Implementation (deep): RateLimiter (token bucket RPM + daily counter)
            + RetryExecutor (exponential backoff + jitter + Retry-After)
            + CircuitBreaker (CLOSED/OPEN/HALF_OPEN state machine)
            + HTTPClient (urllib request execution)
            + Structured JSON logging (every request timed and logged)

    Execution order per request:
        1. RateLimiter.acquire() — throttle to respect RPM/daily limits
        2. CircuitBreaker.call() — fail fast if upstream is down
        3. RetryExecutor.execute() — retry transient failures with backoff
        4. HTTPClient.request() — actual HTTP call via urllib
        5. Log structured JSON with timing, status, rate limit status

    This class accepts all dependencies via constructor injection for testability.
    """

    def __init__(
        self,
        rate_limiter=None,
        retry_executor=None,
        circuit_breaker=None,
        http_client=None,
    ):
        """Initialize resilient client with injectable dependencies.

        Args:
            rate_limiter: RateLimiter instance. Defaults to unlimited.
            retry_executor: RetryExecutor instance. Defaults to 3 retries.
            circuit_breaker: CircuitBreaker instance. Defaults to threshold=5.
            http_client: HTTPClient instance. Defaults to new HTTPClient().
        """
        self._rate_limiter = rate_limiter or RateLimiter()
        self._retry = retry_executor or RetryExecutor()
        self._circuit_breaker = circuit_breaker or CircuitBreaker()
        self._http = http_client or HTTPClient()

        # Request counters for monitoring
        self._total_requests = 0
        self._total_retries = 0
        self._total_errors = 0

    def request(
        self,
        method,
        url,
        headers=None,
        params=None,
        json_body=None,
        timeout=30,
        ssl_context=None,
    ):
        """Execute HTTP request with rate limiting, retry, and circuit breaker.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: Full URL to request.
            headers: Optional HTTP headers dict.
            params: Optional query parameters dict.
            json_body: Optional JSON body dict.
            timeout: Request timeout in seconds.
            ssl_context: Optional SSL context for HTTPS.

        Returns:
            dict: Response with 'status_code', 'headers', 'body' keys.

        Raises:
            RateLimitExceededError: Daily quota exhausted.
            CircuitBreakerOpenError: Circuit is OPEN.
            APIHTTPError: After all retries exhausted on retryable error,
                or immediately on non-retryable error.
            APINetworkError: Network-level failure.
        """
        start_time = time.monotonic()
        self._total_requests += 1

        # Step 1: Acquire rate limit token (may block/sleep)
        self._rate_limiter.acquire()

        # Step 2+3: Circuit breaker wraps retry wraps HTTP call
        def _do_request():
            return self._http.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
                timeout=timeout,
                ssl_context=ssl_context,
            )

        def _with_circuit_breaker():
            return self._circuit_breaker.call(_do_request)

        try:
            response = self._retry.execute(_with_circuit_breaker)
            duration_ms = (time.monotonic() - start_time) * 1000

            self._log_request(
                method, url,
                response.get("status_code"),
                duration_ms,
            )
            return response

        except Exception as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            self._total_errors += 1

            self._log_request(
                method, url,
                getattr(e, 'status_code', None),
                duration_ms,
                error=str(e),
            )
            raise

    def _log_request(self, method, url, status_code, duration_ms, error=None):
        """Log structured JSON for every HTTP request."""
        log_entry = {
            "event": "http_request",
            "method": method,
            "url": url,
            "status_code": status_code,
            "duration_ms": round(duration_ms, 1),
            "request_number": self._total_requests,
            "rate_limit": self._rate_limiter.get_status(),
            "circuit_breaker": self._circuit_breaker.get_status(),
        }

        if error:
            log_entry["error"] = error
            logger.error(json.dumps(log_entry))
        else:
            logger.info(json.dumps(log_entry))

    def get_stats(self):
        """Return cumulative client statistics for job monitoring.

        Returns:
            dict: Total requests, retries, errors, and component statuses.
        """
        return {
            "total_requests": self._total_requests,
            "total_errors": self._total_errors,
            "rate_limiter": self._rate_limiter.get_status(),
            "circuit_breaker": self._circuit_breaker.get_status(),
        }
