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
    Production grade HTTP client with full reliability stack.
    Implementation : RateLimiter
    + RetryExecutor (exponential backoff + Jitter + retry-after)
    + CircuitBreaker (CLOSED/OPEN/HALF_OPEN state machine)
    + HTTP client : urllib request execution
    + Structured JSON logging 
    """
    def __init__(self, rate_limiter=None, retry_executor=None, circuit_breaker=None, http_client=None):
        self._rate_limiter = rate_limiter or RateLimiter()
        self._retry = retry_executor or RetryExecutor()
        self._circuit_breaker = circuit_breaker or CircuitBreaker()
        self._http = http_client or HTTPClient()

        self._total_requests = 0
        self._total_retries = 0
        self._total_errors = 0

    def request(self, method, url, headers=None, params=None, json_body=None, timeout=30, ssl_context=None):
        """Return with status code, headers, body, keys"""
        start_time = time.monotonic()
        self._total_requests += 1

        self._rate_limiter.acquire()

        def _do_request():
            return self._http.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
                timeout=timeout,
                ssl_context=ssl_context
            )

        def _with_circuit_breaker():
            return self._circuit_breaker.call(_do_request)

        try:
            response = self._retry.execute(_with_circuit_breaker)
            duration_ms = (time.monotonic() - start_time) * 1000

            self._log_request(
                method, url, response.get("status_code"),
                duration_ms
            )
            return response
        except Exception as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            self._total_errors += 1

            self._log_request(
                method, url, getattr(e, 'status_code', None),
                duration_ms, error=str(e) 
            )
            raise

    def _log_request(self, method, url, status_code, duration_ms, error=None):
        log_entry = {
            "event": "http_request",
            "method": method,
            "url": url,
            "status_code": status_code,
            "duration_ms": round(duration_ms, 1),
            "request_number": self._total_requests,
            "rate_limit": self._rate_limiter.get_status(),
            "circuit_breaker": self._circuit_breaker.get_status()
        }

        if error: 
            log_entry["error"] = error
            logger.error(json.dumps(log_entry))
        else:
            logger.info(json.dumps(log_entry))

    def get_stats(self):
        """
        Return total request, retries, errors, and component sattuses
        """
        return {
            "total_requests": self._total_requests,
            "total_errors": self._total_errors,
            "rate_limiter": self._rate_limiter.get_status(),
            "circuit_breaker": self._circuit_breaker.get_status()
        }

        