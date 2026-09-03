import time
import json
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)

class ResilientAPIIngestor:
    """
    Quản lý luồng gọi API an toàn:
    - Tái sử dụng socket qua Persistent Connection Pool.
    - Tự động backoff với Full Jitter khi dính HTTP 429 hoặc 5xx.
    - Đọc header Retry-After để tự điều tiết tốc độ.
    """
    def __init__(self, rate_limit_rps: int = 10, max_pool_size: int = 20):
        self.delay_between_calls = 1.0 / max(rate_limit_rps, 1)
        self.last_call_time = 0.0
        
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(
            pool_connections=max_pool_size,
            pool_maxsize=max_pool_size,
            max_retries=retry_strategy
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def fetch_endpoint(self, url: str, headers: dict = None, params: dict = None) -> dict:
        # Rate Limiter: đảm bảo khoảng cách giữa các request
        elapsed = time.time() - self.last_call_time
        if elapsed < self.delay_between_calls:
            time.sleep(self.delay_between_calls - elapsed)

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=(5, 30))
            self.last_call_time = time.time()

            if response.status_code == 200:
                return {"status": "SUCCESS", "data": response.json(), "url": url}
            
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.warning("[429 Rate Limited] Backing off for %ss on URL: %s", retry_after, url)
                time.sleep(retry_after)
                return self.fetch_endpoint(url, headers, params)
            
            else:
                return {"status": "HTTP_ERROR", "code": response.status_code, "body": response.text, "url": url}

        except Exception as ex:
            logger.error("[Network Exception] %s on URL: %s", str(ex), url)
            return {"status": "EXCEPTION", "error": str(ex), "url": url}

    def close(self):
        self.session.close()
