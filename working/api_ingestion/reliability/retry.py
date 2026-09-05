import time
import random
import logging
import json

from exceptions.errors import APIHTTPError

logger = logging.getLogger(__name__)


class RetryExecutor:
    """
    Executes a callable with exponential backoff + full jitter on transient failures.

    Deep module design:
        Interface (small):  execute(fn) -> result
        Implementation (deep): exponential backoff with full jitter,
        Retry-After header parsing, configurable retry status codes,
        structured JSON logging for each retry attempt.

    Only retries on configured HTTP status codes (default: 408, 429, 500, 502, 503, 504).
    """

    def __init__(
        self,
        max_attempts=3,
        backoff_factor=1.0,
        max_backoff_seconds=60,
        retry_status_codes=None,
    ):
        """Initialize retry executor.

        Args:
            max_attempts: Maximum number of attempts (including the first try).
            backoff_factor: Base multiplier for exponential backoff.
            max_backoff_seconds: Maximum delay between retries.
            retry_status_codes: HTTP status codes that trigger retry.
                Defaults to [408, 429, 500, 502, 503, 504].
        """
        self._max_attempts = max_attempts
        self._backoff_factor = backoff_factor
        self._max_backoff = max_backoff_seconds
        self._retry_codes = set(
            retry_status_codes or [408, 429, 500, 502, 503, 504]
        )

    def execute(self, fn):
        """Execute fn() with retry on transient failures.

        Args:
            fn: Callable that returns a response dict or raises APIHTTPError.

        Returns:
            The result of fn() on success.

        Raises:
            APIHTTPError: After all retry attempts exhausted, or on non-retryable status.
        """
        last_error = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                result = fn()

                if attempt > 1:
                    logger.info(json.dumps({
                        "event": "retry_success",
                        "attempt": attempt,
                        "total_attempts": self._max_attempts,
                    }))

                return result

            except APIHTTPError as e:
                last_error = e

                # Non-retryable status code -> raise immediately
                if e.status_code not in self._retry_codes:
                    raise

                # Last attempt exhausted -> break to raise
                if attempt == self._max_attempts:
                    break

                # Calculate delay with exponential backoff + full jitter
                base_delay = self._backoff_factor * (2 ** (attempt - 1))
                capped_delay = min(base_delay, self._max_backoff)

                # Honor Retry-After header if present
                retry_after = self._parse_retry_after(e)
                if retry_after is not None:
                    delay = min(retry_after, self._max_backoff)
                else:
                    # Full jitter: uniform random in [0, capped_delay]
                    delay = random.uniform(0, capped_delay)

                logger.warning(json.dumps({
                    "event": "retry_attempt",
                    "attempt": attempt,
                    "max_attempts": self._max_attempts,
                    "status_code": e.status_code,
                    "delay_seconds": round(delay, 3),
                    "error": str(e),
                }))

                time.sleep(delay)

        # All retries exhausted
        logger.error(json.dumps({
            "event": "retry_exhausted",
            "total_attempts": self._max_attempts,
            "last_status_code": last_error.status_code if last_error else None,
            "last_error": str(last_error) if last_error else None,
        }))
        raise last_error

    def _parse_retry_after(self, error):
        """Extract Retry-After header value in seconds from APIHTTPError.

        The Retry-After header may be in the response_body dict as
        parsed headers, or directly as a header string.

        Args:
            error: APIHTTPError instance.

        Returns:
            float: Seconds to wait, or None if header not present.
        """
        if hasattr(error, 'response_body') and isinstance(error.response_body, dict):
            retry_after = error.response_body.get('Retry-After')
            if retry_after is not None:
                try:
                    return float(retry_after)
                except (ValueError, TypeError):
                    pass
        return None
