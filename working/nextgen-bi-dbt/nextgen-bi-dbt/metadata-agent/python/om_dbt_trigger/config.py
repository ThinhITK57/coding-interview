from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TriggerConfig:
    api_base_url: str
    jwt_token: str
    database_service_name: str
    trigger_mode: str
    pipeline_name: str | None
    verify_ssl: bool
    timeout_seconds: int
    retry_max_attempts: int
    retry_initial_delay_seconds: float
    retry_max_delay_seconds: float

    @staticmethod
    def _truthy(value: str) -> bool:
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _build_api_base_url() -> str:
        api_hostport = os.getenv("OM_API_HOSTPORT", "").strip()
        if api_hostport:
            return api_hostport.rstrip("/")

        om_host = os.getenv("OM_HOST", "").strip().rstrip("/")
        if not om_host:
            raise ValueError("OM_HOST or OM_API_HOSTPORT is required")

        return f"{om_host}/api"

    @classmethod
    def from_env(cls) -> "TriggerConfig":
        api_base_url = cls._build_api_base_url()

        jwt_token = os.getenv("OM_JWT", "").strip()
        if not jwt_token:
            raise ValueError("OM_JWT is required")

        database_service_name = os.getenv("OM_DATABASE_SERVICE_NAME", "").strip()
        if not database_service_name:
            raise ValueError("OM_DATABASE_SERVICE_NAME is required")

        raw_mode = os.getenv("OM_DBT_TRIGGER_MODE", "").strip().lower()
        pipeline_name = os.getenv("OM_DBT_PIPELINE_NAME", "").strip() or None

        trigger_mode = raw_mode or ("name" if pipeline_name else "all")
        if trigger_mode not in {"all", "first", "name"}:
            raise ValueError("OM_DBT_TRIGGER_MODE must be one of: all, first, name")

        if trigger_mode == "name" and not pipeline_name:
            raise ValueError("OM_DBT_PIPELINE_NAME is required when OM_DBT_TRIGGER_MODE=name")

        verify_ssl_raw = os.getenv("OM_VERIFY_SSL", "true").strip().lower()
        verify_ssl = verify_ssl_raw not in {"ignore", "false", "0", "no", "off"}

        timeout_raw = os.getenv("OM_REQUEST_TIMEOUT", "30").strip()
        timeout_seconds = int(timeout_raw)
        if timeout_seconds <= 0:
            raise ValueError("OM_REQUEST_TIMEOUT must be > 0")

        retry_max_attempts_raw = os.getenv("OM_RETRY_MAX_ATTEMPTS", "4").strip()
        retry_max_attempts = int(retry_max_attempts_raw)
        if retry_max_attempts <= 0:
            raise ValueError("OM_RETRY_MAX_ATTEMPTS must be > 0")

        retry_initial_delay_raw = os.getenv("OM_RETRY_INITIAL_DELAY_SECONDS", "1").strip()
        retry_initial_delay_seconds = float(retry_initial_delay_raw)
        if retry_initial_delay_seconds < 0:
            raise ValueError("OM_RETRY_INITIAL_DELAY_SECONDS must be >= 0")

        retry_max_delay_raw = os.getenv("OM_RETRY_MAX_DELAY_SECONDS", "8").strip()
        retry_max_delay_seconds = float(retry_max_delay_raw)
        if retry_max_delay_seconds < retry_initial_delay_seconds:
            raise ValueError("OM_RETRY_MAX_DELAY_SECONDS must be >= OM_RETRY_INITIAL_DELAY_SECONDS")

        return cls(
            api_base_url=api_base_url,
            jwt_token=jwt_token,
            database_service_name=database_service_name,
            trigger_mode=trigger_mode,
            pipeline_name=pipeline_name,
            verify_ssl=verify_ssl,
            timeout_seconds=timeout_seconds,
            retry_max_attempts=retry_max_attempts,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
        )
