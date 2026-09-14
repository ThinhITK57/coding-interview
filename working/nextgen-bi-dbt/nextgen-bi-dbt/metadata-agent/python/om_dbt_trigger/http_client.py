from __future__ import annotations

import json
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class HttpError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, retryable: bool = False) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


class JsonHttpClient:
    def __init__(
        self,
        base_url: str,
        jwt_token: str,
        verify_ssl: bool,
        timeout_seconds: int,
        retry_max_attempts: int = 4,
        retry_initial_delay_seconds: float = 1,
        retry_max_delay_seconds: float = 8,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._jwt_token = jwt_token
        self._timeout_seconds = timeout_seconds
        self._ssl_context = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
        self._retry_max_attempts = retry_max_attempts
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds

    def get(self, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
        return self._request("GET", path, query=query)

    def post(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request("POST", path, body=body or {})

    def _request(
        self,
        method: str,
        path: str,
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_error: HttpError | None = None
        for attempt in range(1, self._retry_max_attempts + 1):
            try:
                return self._request_once(method=method, path=path, query=query, body=body)
            except HttpError as exc:
                last_error = exc
                if not exc.retryable or attempt >= self._retry_max_attempts:
                    raise

                sleep_seconds = self._calculate_backoff(attempt)
                print(
                    f"[metadata-agent] transient OpenMetadata error, retry {attempt}/{self._retry_max_attempts} "
                    f"in {sleep_seconds:.1f}s: {exc}"
                )
                time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error

    def _request_once(
        self,
        method: str,
        path: str,
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        if query:
            encoded_query = urllib.parse.urlencode(query)
            url = f"{url}?{encoded_query}"

        raw_data = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        if body is not None:
            raw_data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(url, method=method, data=raw_data, headers=headers)

        try:
            with urllib.request.urlopen(
                request,
                timeout=self._timeout_seconds,
                context=self._ssl_context,
            ) as response:
                raw_payload = response.read().decode("utf-8").strip()
        except urllib.error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            retryable = exc.code >= 500
            raise HttpError(
                f"HTTP {exc.code} {method} {url}: {message}",
                status_code=exc.code,
                retryable=retryable,
            ) from exc
        except urllib.error.URLError as exc:
            retryable = isinstance(exc.reason, TimeoutError | socket.timeout)
            raise HttpError(f"Network error {method} {url}: {exc}", retryable=retryable) from exc
        except TimeoutError as exc:
            raise HttpError(f"Timeout {method} {url}: {exc}", retryable=True) from exc
        except socket.timeout as exc:
            raise HttpError(f"Timeout {method} {url}: {exc}", retryable=True) from exc

        if not raw_payload:
            return {}

        try:
            return json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise HttpError(f"Invalid JSON response from {method} {url}") from exc

    def _calculate_backoff(self, attempt: int) -> float:
        if self._retry_initial_delay_seconds <= 0:
            return 0

        delay = self._retry_initial_delay_seconds * (2 ** (attempt - 1))
        return min(delay, self._retry_max_delay_seconds)
