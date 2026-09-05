import time
import threading
import logging
import json
from datetime import date

from exceptions.errors import RateLimitExceededError

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Dual-layer rate limiter: token bucket for per-minute + daily counter.

    Deep module design:
        Interface (small):  acquire() — blocks until a token is available.
        Implementation (deep): token bucket algorithm, daily counter with
        midnight reset, structured JSON logging, thread-safe locks.

    Raises RateLimitExceededError when daily quota is exhausted.
    """

    def __init__(self, requests_per_minute=None, requests_per_day=None):
        """Initialize rate limiter with optional per-minute and daily limits.

        Args:
            requests_per_minute: Maximum requests per minute (token bucket). None = unlimited.
            requests_per_day: Maximum requests per day (hard cap). None = unlimited.
        """
        self._rpm = requests_per_minute
        self._daily_limit = requests_per_day

        # Token bucket for per-minute limiting
        if requests_per_minute is not None:
            self._max_tokens = float(requests_per_minute)
            self._tokens = float(requests_per_minute)
            self._refill_rate = requests_per_minute / 60.0  # tokens per second
        else:
            self._max_tokens = float('inf')
            self._tokens = float('inf')
            self._refill_rate = float('inf')

        self._last_refill_time = time.monotonic()

        # Daily counter
        self._daily_count = 0
        self._current_date = date.today()

        self._lock = threading.Lock()

    def acquire(self):
        """Acquire a request token. Blocks if per-minute rate is exceeded.

        Raises:
            RateLimitExceededError: If daily limit is exhausted.
        """
        wait_time = 0.0

        with self._lock:
            self._reset_daily_if_needed()
            self._refill_tokens()

            # Check daily limit first (hard cap)
            if self._daily_limit is not None and self._daily_count >= self._daily_limit:
                raise RateLimitExceededError(
                    f"Daily API quota exhausted: {self._daily_count}/{self._daily_limit}"
                )

            # Check per-minute tokens
            if self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / self._refill_rate

        # Sleep OUTSIDE the lock to not block other threads
        if wait_time > 0:
            self._log_wait(wait_time)
            time.sleep(wait_time)

        # Consume token after waiting
        with self._lock:
            self._refill_tokens()
            self._tokens -= 1.0
            self._daily_count += 1

    def _refill_tokens(self):
        """Refill token bucket based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill_time
        self._tokens = min(
            self._max_tokens,
            self._tokens + elapsed * self._refill_rate
        )
        self._last_refill_time = now

    def _reset_daily_if_needed(self):
        """Reset daily counter at midnight."""
        today = date.today()
        if today != self._current_date:
            logger.info(json.dumps({
                "event": "daily_counter_reset",
                "previous_date": str(self._current_date),
                "previous_count": self._daily_count,
                "new_date": str(today)
            }))
            self._daily_count = 0
            self._current_date = today

    def _log_wait(self, wait_seconds):
        """Log structured JSON when rate limiter forces a wait."""
        logger.info(json.dumps({
            "event": "rate_limit_wait",
            "wait_seconds": round(wait_seconds, 3),
            "rpm_remaining": max(0, int(self._tokens)),
            "daily_remaining": (
                self._daily_limit - self._daily_count
            ) if self._daily_limit is not None else None
        }))

    def get_status(self):
        """Return current rate limiter status for monitoring.

        Returns:
            dict: Status with rpm_remaining, daily_used, daily_remaining.
        """
        with self._lock:
            self._reset_daily_if_needed()
            self._refill_tokens()
            return {
                "rpm_remaining": max(0, int(self._tokens)),
                "rpm_limit": self._rpm,
                "daily_used": self._daily_count,
                "daily_limit": self._daily_limit,
                "daily_remaining": (
                    self._daily_limit - self._daily_count
                ) if self._daily_limit is not None else None,
            }
