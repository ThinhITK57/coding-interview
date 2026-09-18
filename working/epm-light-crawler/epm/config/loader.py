import json
import os
from typing import Dict, List, Optional
from epm.monitoring.metrics import EndpointConfig, RateLimitConfig, RetryConfig, PaginationConfig


class ConfigLoader:
    def __init__(self, config_path: str = "config.json"):
        # Auto-resolve if config.json is in current dir or epm/
        if not os.path.isabs(config_path):
            candidates = [
                os.path.abspath(config_path),
                os.path.abspath(os.path.join("epm", config_path)),
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", config_path)),
            ]
            found = None
            for c in candidates:
                if os.path.exists(c):
                    found = c
                    break
            self.config_path = found or os.path.abspath(config_path)
        else:
            self.config_path = config_path

    def load(self) -> Dict[str, EndpointConfig]:
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        api_data = data.get("api", {})
        base_url = api_data.get("base_url", "https://api2.clarizen.com")
        endpoints_data = api_data.get("endpoints", [])

        endpoints: Dict[str, EndpointConfig] = {}
        for ep in endpoints_data:
            name = ep.get("name")
            rl_data = ep.get("rate_limit", {})
            rate_limit = RateLimitConfig(
                requests_per_minute=rl_data.get("requests_per_minute", 60),
                requests_per_day=rl_data.get("requests_per_day", 1000),
            )

            retry_data = ep.get("retry", {})
            retry = RetryConfig(
                max_attempts=retry_data.get("max_attempts", 5),
                backoff_factor=retry_data.get("backoff_factor", 2.0),
                max_backoff_seconds=retry_data.get("max_backoff_seconds", 60.0),
                retry_status_codes=retry_data.get("retry_status_codes", [429, 500, 502, 503, 504]),
            )

            pg_data = ep.get("pagination", {})
            pagination = PaginationConfig(
                type=pg_data.get("type", "offset"),
                location=pg_data.get("location", "body"),
                page_size=pg_data.get("page_size", 250),
                max_pages=pg_data.get("max_pages", 10000),
                offset_param=pg_data.get("offset_param", "from"),
                limit_param=pg_data.get("limit_param", "limit"),
            )

            auth_data = ep.get("auth", {})
            cred = auth_data.get("credentials", {})
            auth_header = cred.get("header_name", "Authorization")
            auth_env = cred.get("env_name", "PROJECT_API_KEY")
            auth_prefix = cred.get("prefix", "ApiKey")

            endpoint_config = EndpointConfig(
                name=name,
                path=ep.get("path", "/v2.0/services/data/entityQuery"),
                method=ep.get("method", "POST"),
                headers=ep.get("headers", {"Content-Type": "application/json"}),
                auth_type=auth_data.get("type", "api_key"),
                auth_header=auth_header,
                auth_env_name=auth_env,
                auth_prefix=auth_prefix,
                timeout_seconds=ep.get("timeout_seconds", 30),
                rate_limit=rate_limit,
                retry=retry,
                pagination=pagination,
                body=ep.get("body", {}),
                params=ep.get("params", {}),
            )
            endpoints[name] = endpoint_config

        return endpoints
