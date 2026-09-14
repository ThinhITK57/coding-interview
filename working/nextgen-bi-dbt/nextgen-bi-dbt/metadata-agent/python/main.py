from __future__ import annotations

import sys

from om_dbt_trigger.config import TriggerConfig
from om_dbt_trigger.http_client import JsonHttpClient
from om_dbt_trigger.repository import OpenMetadataRepository
from om_dbt_trigger.service import DbtPipelineTriggerService


def main() -> int:
    try:
        config = TriggerConfig.from_env()
        client = JsonHttpClient(
            base_url=config.api_base_url,
            jwt_token=config.jwt_token,
            verify_ssl=config.verify_ssl,
            timeout_seconds=config.timeout_seconds,
            retry_max_attempts=config.retry_max_attempts,
            retry_initial_delay_seconds=config.retry_initial_delay_seconds,
            retry_max_delay_seconds=config.retry_max_delay_seconds,
        )
        repository = OpenMetadataRepository(client)
        service = DbtPipelineTriggerService(config, repository)

        triggered_pipelines = service.execute()
    except Exception as exc:  # noqa: BLE001
        print(f"[metadata-agent] OpenMetadata DBT trigger failed: {exc}", file=sys.stderr)
        return 1

    print("[metadata-agent] OpenMetadata DBT trigger success")
    for item in triggered_pipelines:
        print(f"[metadata-agent] triggered pipeline: {item}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
