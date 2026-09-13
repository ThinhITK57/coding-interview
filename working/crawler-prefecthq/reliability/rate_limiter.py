import time
import threading
import logging
import json

from datetime import date

from exceptions.errors import RateLimitExceededError


logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, requests_per_minute=None, requests_per_day=None):
        """
        Initialize rate limiter with optional per-minute and daily limits
        Args:
            request_per_minute: Maximum requests per minute, if None = Unlimited
            request_per_day: Maximum requests per day, if None=unlimited
        """
        self._rpm = requests_per_minute
        self._daily_limit = requests_per_day

        if requests_per_minute is not None:
            self._max_tokens = float(requests_per_minute)
            self._tokens = float(requests_per_minute)
            self._refill_rate = requests_per_minute / 60.0

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
        """Blocks if per-minute rate is exceeded"""
        wait_time = 0.0

        with self._lock:
            self._reset_daily_if_needed()
            self._refill_tokens()

            if self._daily_limit is not None and self._daily_count >= self._daily_limit:
                raise RateLimitExceededError(
                    f"Daily API quota exhausted: {self._daily_count} / {self._daily_limit}"
                )
            if self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / self._refill_rate
        if wait_time > 0:
            self._log_wait(wait_time)
            time.sleep(wait_time)

        # Consume token after waiting
        with self._lock:
            self._refill_tokens()
            self._tokens -= 1.0
            self._daily_count += 1

    def _refill_tokens(self):
        now = time.monotonic()
        elapsed = now - self._last_refill_time
        self._tokens = min(
            self._max_tokens,
            self._tokens + elapsed*self._refill_rate
        )
        self._last_refill_time = now

    def _reset_daily_if_needed(self):
        today = date.today()
        if today != self._current_date:
            logger.info(
                json.dumps(
                    {
                        "event": "daily_counter_reset",
                        "previous_date": str(self._current_date),
                        "previous_count": self._daily_count,
                        "new_date": str(today)
                    }
                )
            )
            self._daily_count = 0
            self._current_date = today


    def _log_wait(self, wait_seconds):
        """Log structured JSON when rate limiter forces a wait"""
        logger.info(
            json.dumps(
                {
                    "event": "rate_limit_wait",
                    "wait_seconds": round(wait_seconds, 3),
                    "rpm_remaining": max(0, int(self._tokens)),
                    "daily_remaining": (self._daily_limit - self._daily_count) if self._daily_limit is not None else None
                }
            )
        )

    def get_status(self):
        """Return current rate limiter status for monitoring.
            return : dict with rpm_remaining, daily_used, daily_remaining
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
                ) if self._daily_limit is not None else None
            }