from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineInfo:
    pipeline_id: str
    name: str
    fully_qualified_name: str | None
    pipeline_type: str | None
    source_type: str | None

    @property
    def display_name(self) -> str:
        return self.fully_qualified_name or self.name
