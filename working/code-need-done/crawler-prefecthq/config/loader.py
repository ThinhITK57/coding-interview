import os
import json
import re
from . import IngestionConfig


from .api_config import (
    APIConfig,
    EndpointConfig,
    AuthConfig,
    RateLimitConfig,
    RetryConfig,
    PaginationConfig,
    HttpMethod,
    AuthType,
    PaginationType,
    HttpMethod,
    AuthType,
    PaginationType
)

from .job_config import (
    JobConfig,
    ExtractionConfig,
    CheckpointConfig,
    StorageConfig,
    ExtractionMode,
    StorageFormat,
    IncrementalConfig
)
from utils.env_loader import load_env

ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def resolve_endpoint_name(name, endpoints, registry_path="tables_registry.json"):
    """Doi ten nguoi dung go thanh ten endpoint that trong config.json.

    Truoc day cho nay dung mot bang alias viet cung ('objective' -> 'bsc').
    Moi lan doi ten endpoint trong config la bang do lech, va pipeline chet voi
    "job endpoint bsc does not exist" du 'objective' van ton tai. Nen quy tac
    dau tien la: ten nao da khop san thi TRA VE NGUYEN, khong doi gi ca.

    Tra ve None neu khong khop endpoint nao, de goi y ten dung cho nguoi dung.
    """
    if not name:
        return None

    actual = {ep.name.lower(): ep.name for ep in endpoints}
    key = str(name).strip().lower()

    # 1. Khop truc tiep - uu tien tuyet doi
    if key in actual:
        return actual[key]

    # 2. Doi chieu qua aliases trong tables_registry.json
    if os.path.exists(registry_path):
        try:
            with open(registry_path, "r", encoding="utf-8") as handle:
                tables = json.load(handle).get("tables", [])
            for table in tables:
                names = [str(n).lower()
                         for n in [table.get("table_name")] + list(table.get("aliases") or [])
                         if n]
                if key not in names:
                    continue
                for candidate in names:
                    if candidate in actual:
                        return actual[candidate]
        except Exception:
            pass

    return None


class ConfigLoader:
    def load(self, path: str, env_path: str = ".env") -> IngestionConfig:
        load_env(env_path)
        with open(path, "r") as file:
            raw_config = json.load(file)

        raw_config = self._resolve_environment_variables(
            raw_config
        )
        registry_path = os.path.join(os.path.dirname(path) or ".", "tables_registry.json")
        if os.path.exists(registry_path):
            try:
                with open(registry_path, "r", encoding="utf-8") as rf:
                    registry_data = json.load(rf)
                self._merge_tables_registry(raw_config, registry_data)
            except Exception:
                pass
        return self._build_config(raw_config)

    def _merge_tables_registry(self, raw_config, registry_data):
        tables = registry_data.get("tables", [])
        endpoints = raw_config.get("api", {}).get("endpoints", [])

        for table in tables:
            t_name = table.get("table_name")
            entity_type = table.get("entity_type")
            fields = table.get("fields", [])

            if not fields:
                continue

            for ep in endpoints:
                ep_name = ep.get("name")
                body = ep.get("body")

                if not isinstance(body, dict):
                    continue
                body_type = body.get("typeName")
                if ep_name == t_name or (
                        entity_type and body_type == entity_type
                ):
                    # body_type = body.get("typeName")
                    # if ep_name == t_name or (entity_type and body_type == entity_type):
                    #     body["fields"] = fields
                    if not body.get("fields"):
                        body["fields"] = registry_fields

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
                if env_name in ("WINDOW_START", "WINDOW_END"):
                    return os.getenv(env_name, f"${{{env_name}}}")

                env_value = os.getenv(env_name)

                if env_value is None:
                    raise ValueError(
                        f"Environment variable '{env_name}' "
                        f"is not defined"
                    )

                return env_value

            return ENV_PATTERN.sub(replace_env, value)

        return value

    def _build_config(self, raw)-> IngestionConfig:
        api_config = self._build_api_config(raw["api"])

        job_config = self._build_job_config(raw["job"])

        return IngestionConfig(
            api=api_config,
            job=job_config
        )

    def _build_api_config(self, config: dict)-> APIConfig:
        endpoints = []
        for raw_endpoint in config.get(
            "endpoints", []
        ):
            endpoints.append(
                self._build_endpoint_config(
                    raw_endpoint
                )
            )
        return APIConfig(
            name=config["name"],
            version=config["version"],
            base_url=config["base_url"],
            endpoints=endpoints
        )

    def _build_endpoint_config(self, config):
        return EndpointConfig(
            name=config["name"],
            path=config["path"],
            method=HttpMethod(
                config.get("method", "GET")
            ),
            headers=config.get(
                "headers", {}
            ),
            params=config.get(
                "params", {}
            ),
            body=config.get("body"),
            timeout_seconds=config.get("timeout_seconds", 30),
            auth=self._build_auth_config(config.get("auth")),
            rate_limit=self._build_rate_limit_config(config.get("rate_limit")),
            retry=self._build_retry_config(config.get("retry")),
            pagination=self._build_pagination_config(config.get("pagination"))
        )

    def _build_auth_config(self, config):
        if not config:
            return None

        return AuthConfig(
            type=AuthType(config["type"]),
            credentials=config.get("credentials", {})
        )

    def _build_rate_limit_config(self, config):
        if not config:
            return None

        return RateLimitConfig(
            requests_per_minute=config.get("requests_per_minute"),
            requests_per_day=config.get("requests_per_day"),
            burst_limit=config.get("burst_limit")
        )
    # def _build_endpoint_config_bk(
    #     self,
    #     config: dict
    # )-> EndpointConfig:
    #     auth = None
    #     if config.get("auth"):
    #         auth = AuthConfig(
    #             type = AuthType(
    #                 config["auth"]["type"]
    #             ),
    #             credentials=config["auth"].get(
    #                 "credentials",
    #                 {}
    #             )
    #         )
    #     rate_limit = None
    #     if config.get("rate_limit"):
    #         rate_limit = RateLimitConfig(
    #             requests_per_minute=config["rate_limit"].get("requests_per_minute"),
    #             requests_per_day=config["rate_limit"].get("requests_per_day"),
    #             burst_limit=config["rate_limit"].get(
    #                 "burst_limit"
    #             )
    #         )

    #     retry = self._build_retry_config(
    #         config.get("retry", {})
    #     )

    #     pagination = self._build_pagination_config(
    #         config.get("pagination")
    #     )

    #     return EndpointConfig(
    #         name=config["name"],
    #         path=config["path"],
    #         method=HttpMethod(
    #             config.get("method", "GET")
    #         ),
    #         headers=config.get("headers", {}),
    #         params=config.get("params", {}),
    #         body=config.get("body"),
    #         timeout_seconds=config.get("timeout_seconds", 30),
    #         auth=auth,
    #         rate_limit=rate_limit,
    #         retry=retry,
    #         pagination=pagination,
    #     )
    

    def _build_retry_config(self, config: dict) -> RetryConfig:
        return RetryConfig(
            max_attempt=config.get("max_attempts", 3),
            backoff_factor=config.get("backoff_factor", 1.0),
            max_backoff_seconds=config.get("max_backoff_seconds", 60),
            retry_status_codes=config.get(
                "retry_status_code",
                [
                    408,
                    429,
                    500,
                    502,
                    503,
                    504
                ]
            )
        )

    def _build_pagination_config(
            self,
            config
    ):
        if not config:
            return None

        return PaginationConfig(
            type=PaginationType(
                config.get(
                    "type",
                    "none"
                )
            ),
            location=config.get("location", "query"),
            page_size=config.get(
                "page_size", 100
            ),
            max_pages=config.get("max_pages"),
            max_records=config.get("max_records"),
            offset_param=config.get("offset_param", "offset"),
            limit_param=config.get("limit_param", "limit"),
            page_param=config.get("page_param", "page"),
            cursor_param=config.get("cursor_param", "cursor")
        )
    
    # def _build_job_config(self, config: dict) -> JobConfig:
    #     extraction = ExtractionConfig(
    #         mode=ExtractionMode(
    #             config.get(
    #                 "extraction", {}
    #             ).get("mode", "full")
    #         ),
    #         batch_size=config.get(
    #             "extraction",
    #             {}
    #         ).get(
    #             "batch_size",
    #             1000
    #         ),

    #         max_records=config.get("extraction", {}).get("max_records"),
    #         max_pages=config.get(
    #             "extraction", {}
    #         ).get("max_pages"),

    #         max_runtime_seconds=config.get(
    #             "extraction", {}
    #         ).get("max_runtime_seconds"),
    #     )

    #     checkpoint_raw = config.get(
    #         "checkpoint",
    #         {}
    #     )

    #     checkpoint = CheckpointConfig(
    #         enabled=checkpoint_raw.get(
    #             "enabled", True
    #         ),
    #         store_type=checkpoint_raw.get("store_type", "file"),
    #         path=checkpoint_raw.get("path")
    #     )

    #     storage_raw = config.get(
    #         "storage", {}
    #     )

    #     storage = StorageConfig(
    #         format=StorageFormat(
    #             storage_raw.get("format", "parquet")
    #         ),
    #         layer=storage_raw.get("layer", "bronze"),
    #         path=storage_raw.get("path", ""),
    #         partition_columns=storage_raw.get("partition_columns", []),
    #         compression=storage_raw.get("compression", "snappy")
    #     )

    #     return JobConfig(
    #         job_name=config["job_name"],
    #         source=config["source"],
    #         endpoint=config["endpoint"],
    #         api_version=config["api_version"],
    #         extraction=extraction,
    #         checkpoint=checkpoint,
    #         storage=storage
    #     )
    def _build_job_config(self, config: dict) -> JobConfig:
        extraction_raw = config.get("extraction", {})

        incremental = self._build_incremental_config(
            extraction_raw.get("incremental", {})
        ) 

        extraction = ExtractionConfig(
            mode=ExtractionMode(
                extraction_raw.get("mode", "full")
            ),
            batch_size=extraction_raw.get(
                "batch_size",
                1000
            ),
            max_records=extraction_raw.get(
                "max_records"
            ),
            max_pages=extraction_raw.get(
                "max_pages"
            ),
            max_runtime_seconds=extraction_raw.get(
                "max_runtime_seconds"
            ),
            incremental=incremental
        )

        checkpoint_raw = config.get(
            "checkpoint",
            {}
        )

        checkpoint = CheckpointConfig(
            enabled=checkpoint_raw.get(
                "enabled", True
            ),
            store_type=checkpoint_raw.get(
                "store_type", "file"
            ),
            path=checkpoint_raw.get("path")
        )

        storage_raw = config.get(
            "storage",
            {}
        )

        storage = StorageConfig(
            format=StorageFormat(
                storage_raw.get(
                    "format",
                    "parquet"
                )
            ),
            layer=storage_raw.get(
                "layer",
                "bronze"
            ),
            path=storage_raw.get(
                "path",
                ""
            ),
            partition_columns=storage_raw.get(
                "partition_columns",
                []
            ),
            compression=storage_raw.get(
                "compression",
                "snappy"
            )
        )

        return JobConfig(
            job_name=config["job_name"],
            source=config["source"],
            endpoint=config["endpoint"],
            api_version=config["api_version"],
            extraction=extraction,
            checkpoint=checkpoint,
            storage=storage
        )


    def _resolve_environment_variables(self, value):
        if isinstance(value, dict):
            return {key: self._resolve_environment_variables(val) for key, val in value.items()}

        if isinstance(value, list):
            return [
                self._resolve_environment_variables(
                    item
                )
                for item in value
            ]

        if isinstance(value, str):
            pattern = r"\$\{([^}]+)\}"

            def replace(match):
                variable_name = match.group(1)
                result = os.getenv(
                    variable_name
                )
                if result is None:
                    raise ValueError(
                        f"Environment variable "
                        f"'{variable_name}'"
                        f"is not defined"
                    )
                return result
            return re.sub(
                pattern,
                replace, 
                value
            )
        return value

    def _build_incremental_config(self, config):
        if not config:
            return IncrementalConfig()

        return IncrementalConfig(
            watermark_field=config.get(
                "watermark_field", "LastModified"
            ),
            lookback_minutes=config.get(
                "lookback_minutes",
                15
            ),
            datetime_format=config.get(
                "datetime_format", "%Y-%m-%dT%H:%M:%SZ"
            ),
            initial_start_time=config.get(
                "initial_start_time"
            )
        )