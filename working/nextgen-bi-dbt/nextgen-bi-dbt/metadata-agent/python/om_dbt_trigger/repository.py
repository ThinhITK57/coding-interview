from __future__ import annotations

import urllib.parse
from typing import Any

from .http_client import JsonHttpClient
from .models import PipelineInfo


class OpenMetadataRepository:
    def __init__(self, client: JsonHttpClient) -> None:
        self._client = client

    def get_database_service_id(self, service_name: str) -> str:
        encoded_name = urllib.parse.quote(service_name, safe="")
        payload = self._client.get(f"/v1/services/databaseServices/name/{encoded_name}")

        service_id = payload.get("id")
        if not isinstance(service_id, str) or not service_id.strip():
            raise RuntimeError(f"Cannot resolve database service id for '{service_name}'")

        return service_id

    def list_ingestion_pipelines(self, service_id: str) -> list[PipelineInfo]:
        after: str | None = None
        result: list[PipelineInfo] = []

        while True:
            query = {"service": service_id, "limit": "100"}
            if after:
                query["after"] = after

            payload = self._client.get("/v1/services/ingestionPipelines", query=query)
            result.extend(self._parse_pipeline_list(payload.get("data")))

            paging = payload.get("paging")
            if not isinstance(paging, dict):
                break

            next_after = paging.get("after")
            if not isinstance(next_after, str) or not next_after:
                break

            after = next_after

        return result

    def trigger_ingestion_pipeline(self, pipeline_id: str) -> None:
        encoded_id = urllib.parse.quote(pipeline_id, safe="")
        self._client.post(f"/v1/services/ingestionPipelines/trigger/{encoded_id}", body={})

    @staticmethod
    def _parse_pipeline_list(raw_items: Any) -> list[PipelineInfo]:
        if not isinstance(raw_items, list):
            return []

        parsed: list[PipelineInfo] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue

            pipeline_id = item.get("id")
            name = item.get("name")
            if not isinstance(pipeline_id, str) or not isinstance(name, str):
                continue

            source_type = OpenMetadataRepository._extract_source_type(item)
            parsed.append(
                PipelineInfo(
                    pipeline_id=pipeline_id,
                    name=name,
                    fully_qualified_name=item.get("fullyQualifiedName") if isinstance(item.get("fullyQualifiedName"), str) else None,
                    pipeline_type=item.get("pipelineType") if isinstance(item.get("pipelineType"), str) else None,
                    source_type=source_type,
                )
            )

        return parsed

    @staticmethod
    def _extract_source_type(item: dict[str, Any]) -> str | None:
        source_config = item.get("sourceConfig")
        if not isinstance(source_config, dict):
            return None

        config = source_config.get("config")
        if not isinstance(config, dict):
            return None

        source_type = config.get("type")
        if isinstance(source_type, str):
            return source_type

        return None
