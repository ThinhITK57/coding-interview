import time
import random
import logging
import json

from exceptions.errors import APIHTTPError


logger = logging.getLogger(__name__)


class RetryExecutor:
    def __init__(
            self,
            max_attempts=3,
            backoff_factor=1.0,
            max_backoff_seconds=60,
            retry_status_codes=None
        ):
        self._max_attempts = max_attempts
        self._backoff_factor = backoff_factor
        self._max_backoff = max_backoff_seconds
        self._retry_codes = set(
            retry_status_codes or [408, 429, 500, 503, 504]
        )

    def execute(self, fn):
        last_error = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                result = fn()
                if attempt > 1:
                    logger.info(json.dumps(
                        {
                            "event": "retry_success",
                            "attempt": attempt,
                            "total_attempts": self._max_attempts
                        }
                    ))
                return result
            except APIHTTPError as e:
                last_error = e 
                if attempt == self._max_attempts:
                    break

                base_delay = self._backoff_factor * (2**(attempt -1))
                capped_delay = min(base_delay, self._max_backoff)

                # retry after header if present
                retry_after = self._parse_retry_after(e)
                if retry_after is not None:
                    delay = min(retry_after, self._max_backoff)
                else:
                    delay = random.uniform(0, capped_delay)

                logger.warning(json.dumps(
                    {
                        "event": "retry_attempt",
                        "attempt": attempt,
                        "max_attempts": self._max_attempts,
                        "status_code": e.status_code,
                        "delay_seconds": round(delay, 3),
                        "error": str(e)
                    }
                ))
                time.sleep(delay)

        logger.error(json.dumps(
            {
                "event": "retry_exhausted",
                "total_attempts": self._max_attempts,
                "last_status_code": last_error.status_code if last_error else None,
                "last_error": str(last_error) if last_error else  None
            }
        ))
        raise last_error

    def _parse_retry_after(self, error):
        """
        Return second to wait or None if header not present 
        """
        if hasattr(error, 'response_body') and isinstance(error.response_body, dict):
            retry_after = error.response_body.get("Retry-After")
            if retry_after is not None:
                try:
                    return float(retry_after)
                except (ValueError, TypeError):
                    pass
        return None