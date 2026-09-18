import time
import logging
from typing import Optional
from epm.storage.state_store import StateStore, DailyQuotaExceededError

logger = logging.getLogger(__name__)


class RateLimiter:
    """Enforces:
    1. Per-minute rate limit via in-memory sliding delay.
    2. Persistent daily API quota via StateStore (across container restarts).
    """

    def __init__(
        self,
        requests_per_minute: int = 60,
        requests_per_day: int = 1000,
        state_store: Optional[StateStore] = None,
    ):
        self.requests_per_minute = max(1, requests_per_minute)
        self.requests_per_day = max(1, requests_per_day)
        self.interval_seconds = 60.0 / self.requests_per_minute
        self.last_request_time = 0.0
        self.state_store = state_store or StateStore()

    def acquire(self):
        """Blocks to respect per-minute rate limit, then atomically increments daily quota.
        Raises DailyQuotaExceededError if quota for today UTC is exhausted.
        """
        # 1. Enforce per-minute spacing
        now = time.monotonic()
        elapsed = now - self.last_request_time
        if elapsed < self.interval_seconds:
            sleep_time = self.interval_seconds - elapsed
            logger.debug(f"RateLimiter: sleeping {sleep_time:.3f}s for RPM pacing")
            time.sleep(sleep_time)

        # 2. Enforce persistent daily quota in SQLite
        self.state_store.check_and_increment_daily_requests(
            max_limit=self.requests_per_day,
            count=1,
        )

        self.last_request_time = time.monotonic()
