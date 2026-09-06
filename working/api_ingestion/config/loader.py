import json
import os
import re

from config import IngestionConfig

from .api_config import (
    APIConfig,
    EndpointConfig,
    AuthConfig,
    AuthType,
    HttpMethod,
    PaginationConfig,
    PaginationType,
    RateLimitConfig,
    RetryConfig,
)

from .job_config import (
    JobConfig,
    ExtractionConfig,
    ExtractionMode,
    IncrementalConfig,
    CheckpointConfig,
    StorageConfig,
    StorageFormat,
)

from utils.env_loader import load_env


ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


class ConfigLoader:

    def load(
        self,
        path: str,
        env_path: str = ".env",
    ) -> IngestionConfig:

        # Load .env if available
        load_env(env_path)

        # Read JSON config
        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:

            raw_config = json.load(file)

        # ${ENV_VAR}
        raw_config = self._resolve_environment_variables(
            raw_config
        )

        return self._build_config(raw_config)

    def _resolve_environment_variables(self, value):

        if isinstance(value, dict):

            return {
                key: self._resolve_environment_variables(val)
                for key, val in value.items()
            }

        if isinstance(value, list):

            return [
                self._resolve_environment_variables(item)
                for item in value
            ]

        if isinstance(value, str):

            def replace_env(match):

                env_name = match.group(1)

                env_value = os.getenv(env_name)

                if env_value is None:
                    raise ValueError(
                        "Environment variable "
                        f"'{env_name}' is not defined"
                    )

                return env_value

            return ENV_PATTERN.sub(
                replace_env,
                value
            )

        return value

    def _build_config(self, raw):

        return IngestionConfig(
            api=self._build_api_config(
                raw["api"]
            ),
            job=self._build_job_config(
                raw["job"]
            ),
        )

    def _build_api_config(self, config):

        endpoints = [
            self._build_endpoint_config(endpoint)
            for endpoint in config.get(
                "endpoints",
                []
            )
        ]

        return APIConfig(
            name=config["name"],
            version=config["version"],
            base_url=config["base_url"],
            endpoints=endpoints,
        )

    def _build_endpoint_config(self, config):

        return EndpointConfig(
            name=config["name"],
            path=config["path"],

            method=HttpMethod(
                config.get(
                    "method",
                    "GET"
                )
            ),

            headers=config.get(
                "headers",
                {}
            ),

            params=config.get(
                "params",
                {}
            ),

            body=config.get(
                "body"
            ),

            timeout_seconds=config.get(
                "timeout_seconds",
                30
            ),

            auth=self._build_auth_config(
                config.get("auth")
            ),

            rate_limit=self._build_rate_limit_config(
                config.get("rate_limit")
            ),

            retry=self._build_retry_config(
                config.get("retry")
            ),

            pagination=self._build_pagination_config(
                config.get("pagination")
            ),
        )

    def _build_auth_config(self, config):

        if not config:
            return None

        return AuthConfig(
            type=AuthType(
                config["type"]
            ),
            credentials=config.get(
                "credentials",
                {}
            ),
        )

    def _build_rate_limit_config(self, config):

        if not config:
            return None

        return RateLimitConfig(
            requests_per_minute=config.get(
                "requests_per_minute"
            ),
            requests_per_day=config.get(
                "requests_per_day"
            ),
            burst_limit=config.get(
                "burst_limit"
            ),
        )

    def _build_retry_config(self, config):

        if not config:
            return RetryConfig()

        return RetryConfig(
            max_attempts=config.get(
                "max_attempts",
                3
            ),
            backoff_factor=config.get(
                "backoff_factor",
                1.0
            ),
            max_backoff_seconds=config.get(
                "max_backoff_seconds",
                60
            ),
            retry_status_codes=config.get(
                "retry_status_codes",
                [408, 429, 500, 502, 503, 504]
            ),
        )

    def _build_pagination_config(self, config):

        if not config:
            return None

        return PaginationConfig(
            type=PaginationType(
                config.get(
                    "type",
                    "none"
                )
            ),

            location=config.get(
                "location",
                "query"
            ),

            page_size=config.get(
                "page_size",
                100
            ),

            max_pages=config.get(
                "max_pages"
            ),

            max_records=config.get(
                "max_records"
            ),

            offset_param=config.get(
                "offset_param",
                "offset"
            ),

            limit_param=config.get(
                "limit_param",
                "limit"
            ),

            page_param=config.get(
                "page_param",
                "page"
            ),

            cursor_param=config.get(
                "cursor_param",
                "cursor"
            ),
        )

    def _build_job_config(self, config):

        extraction = config.get(
            "extraction",
            {}
        )

        checkpoint = config.get(
            "checkpoint",
            {}
        )

        storage = config.get(
            "storage",
            {}
        )

        return JobConfig(
            job_name=config["job_name"],
            source=config["source"],
            endpoint=config["endpoint"],
            api_version=config["api_version"],

            extraction=ExtractionConfig(
                mode=ExtractionMode(
                    extraction.get(
                        "mode",
                        "full"
                    )
                ),
                batch_size=extraction.get(
                    "batch_size",
                    1000
                ),
                max_records=extraction.get(
                    "max_records"
                ),
                max_pages=extraction.get(
                    "max_pages"
                ),
                max_runtime_seconds=extraction.get(
                    "max_runtime_seconds"
                ),
                window_start=extraction.get(
                    "window_start"
                ),
                window_end=extraction.get(
                    "window_end"
                ),
                incremental=self._build_incremental_config(
                    extraction.get("incremental", {})
                ),
            ),

            checkpoint=CheckpointConfig(
                enabled=checkpoint.get(
                    "enabled",
                    True
                ),
                store_type=checkpoint.get(
                    "store_type",
                    "file"
                ),
                path=checkpoint.get(
                    "path"
                ),
            ),

            storage=StorageConfig(
                format=StorageFormat(
                    storage.get(
                        "format",
                        "parquet"
                    )
                ),
                layer=storage.get(
                    "layer",
                    "bronze"
                ),
                path=storage.get(
                    "path",
                    ""
                ),
                partition_columns=storage.get(
                    "partition_columns",
                    []
                ),
                compression=storage.get(
                    "compression",
                    "snappy"
                ),
            ),
        )

    def _build_incremental_config(self, config):
        if not config:
            return IncrementalConfig()

        return IncrementalConfig(
            watermark_field=config.get(
                "watermark_field",
                "LastModified"
            ),
            lookback_minutes=config.get(
                "lookback_minutes",
                15
            ),
            datetime_format=config.get(
                "datetime_format",
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            initial_start_time=config.get(
                "initial_start_time"
            ),
        )