from __future__ import annotations

from .config import TriggerConfig
from .repository import OpenMetadataRepository
from .selector import PipelineSelector


class DbtPipelineTriggerService:
    def __init__(self, config: TriggerConfig, repository: OpenMetadataRepository) -> None:
        self._config = config
        self._repository = repository

    def execute(self) -> list[str]:
        service_id = self._repository.get_database_service_id(self._config.database_service_name)
        pipelines = self._repository.list_ingestion_pipelines(service_id)

        selected = PipelineSelector.select_dbt_pipelines(
            pipelines=pipelines,
            trigger_mode=self._config.trigger_mode,
            pipeline_name=self._config.pipeline_name,
        )

        triggered: list[str] = []
        for pipeline in selected:
            self._repository.trigger_ingestion_pipeline(pipeline.pipeline_id)
            triggered.append(pipeline.display_name)

        return triggered
