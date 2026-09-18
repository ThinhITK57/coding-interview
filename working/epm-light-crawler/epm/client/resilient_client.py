import json
import time
import random
import logging
from typing import Any, Dict, Optional
import requests

from epm.monitoring.metrics import EndpointConfig
from epm.client.auth import TokenAuth
from epm.reliability.rate_limiter import RateLimiter
from epm.storage.state_store import StateStore, DailyQuotaExceededError

logger = logging.getLogger(__name__)


class APIRequestError(Exception):
    """Raised when an API request fails after all retries."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class ResilientHTTPClient:
    """HTTP Client tailored for Clarizen API with resilient retries,
    per-minute rate limiting, and persistent daily quota enforcement.
    """

    def __init__(
        self,
        endpoint_config: EndpointConfig,
        state_store: Optional[StateStore] = None,
        base_url: str = "https://api2.clarizen.com",
    ):
        self.config = endpoint_config
        self.base_url = base_url.rstrip("/")
        self.auth = TokenAuth(endpoint_config)
        self.state_store = state_store or StateStore()
        self.rate_limiter = RateLimiter(
            requests_per_minute=endpoint_config.rate_limit.requests_per_minute,
            requests_per_day=endpoint_config.rate_limit.requests_per_day,
            state_store=self.state_store,
        )

        self.requests_sent = 0
        self.retry_count = 0
        self.session = requests.Session()

    def request(
        self,
        method: Optional[str] = None,
        path: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        method = (method or self.config.method).upper()
        url = self.base_url + (path or self.config.path)
        req_headers = dict(self.config.headers)
        req_headers.update(self.auth.get_headers())
        if headers:
            req_headers.update(headers)

        max_attempts = self.config.retry.max_attempts
        backoff_factor = self.config.retry.backoff_factor
        max_backoff = self.config.retry.max_backoff_seconds
        retry_codes = set(self.config.retry.retry_status_codes)

        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            # 1. Enforce rate limiting & persistent daily quota
            self.rate_limiter.acquire()
            self.requests_sent += 1

            start_t = time.monotonic()
            try:
                logger.debug(f"Sending {method} request to {url} (attempt {attempt}/{max_attempts})")
                resp = self.session.request(
                    method=method,
                    url=url,
                    headers=req_headers,
                    params=params,
                    json=json_body,
                    timeout=self.config.timeout_seconds,
                )
                duration = time.monotonic() - start_t

                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except json.JSONDecodeError as jde:
                        raise APIRequestError(f"Response 200 is not valid JSON: {jde}")

                if resp.status_code in retry_codes and attempt < max_attempts:
                    self.retry_count += 1
                    sleep_sec = min(max_backoff, (backoff_factor ** (attempt - 1)) + random.uniform(0.1, 1.0))
                    logger.warning(
                        f"HTTP {resp.status_code} from {url}. Retrying in {sleep_sec:.2f}s "
                        f"(attempt {attempt}/{max_attempts}). Body: {resp.text[:200]}"
                    )
                    time.sleep(sleep_sec)
                    continue

                # Non-retriable or exhausted
                raise APIRequestError(
                    f"HTTP {resp.status_code} from {url}: {resp.text[:300]}",
                    status_code=resp.status_code,
                    response_text=resp.text,
                )

            except requests.exceptions.RequestException as e:
                duration = time.monotonic() - start_t
                if attempt < max_attempts:
                    self.retry_count += 1
                    sleep_sec = min(max_backoff, (backoff_factor ** (attempt - 1)) + random.uniform(0.1, 1.0))
                    logger.warning(
                        f"Network error {type(e).__name__}: {e}. Retrying in {sleep_sec:.2f}s "
                        f"(attempt {attempt}/{max_attempts})..."
                    )
                    time.sleep(sleep_sec)
                    continue
                raise APIRequestError(f"Network error after {max_attempts} attempts: {e}")

        raise APIRequestError(f"Failed to fetch {url} after {max_attempts} attempts.")
