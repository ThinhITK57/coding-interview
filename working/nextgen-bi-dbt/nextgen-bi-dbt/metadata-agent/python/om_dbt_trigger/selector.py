from __future__ import annotations

from .models import PipelineInfo


class PipelineSelector:
    @staticmethod
    def select_dbt_pipelines(
        pipelines: list[PipelineInfo],
        trigger_mode: str,
        pipeline_name: str | None,
    ) -> list[PipelineInfo]:
        dbt_pipelines = [pipeline for pipeline in pipelines if PipelineSelector._is_dbt_pipeline(pipeline)]
        if not dbt_pipelines:
            raise RuntimeError("No DBT ingestion pipeline found for the database service")

        if trigger_mode == "all":
            return dbt_pipelines

        if trigger_mode == "first":
            return [dbt_pipelines[0]]

        assert trigger_mode == "name"
        assert pipeline_name

        matched = [
            pipeline
            for pipeline in dbt_pipelines
            if pipeline.name == pipeline_name or pipeline.fully_qualified_name == pipeline_name
        ]
        if not matched:
            raise RuntimeError(f"No DBT ingestion pipeline matched OM_DBT_PIPELINE_NAME='{pipeline_name}'")

        return matched

    @staticmethod
    def _is_dbt_pipeline(pipeline: PipelineInfo) -> bool:
        pipeline_type = (pipeline.pipeline_type or "").strip().lower()
        source_type = (pipeline.source_type or "").strip().upper()
        return pipeline_type == "dbt" or source_type == "DBT"
