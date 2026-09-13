"""Package chứa các module xây dựng 3 bảng Fact Kimball Star Schema và Scenario Engine.

Các Fact gồm:
  - fact_task_execution: Snapshot tiến độ thực thi công việc theo ngày
  - fact_target_snapshot: Snapshot kết quả mục tiêu đa kịch bản (M, N, S)
  - fact_project_progress: Snapshot tổng hợp tiến độ và sức khỏe dự án (health_score)
  
Engine hỗ trợ:
  - ScenarioEngine: Unpivot M/N/S và phân tích khoảng cách mục tiêu (Gap Analysis)
"""

from transform.facts.base_fact import BaseFact
from transform.facts.scenario_engine import ScenarioEngine
from transform.facts.fact_task_execution import FactTaskExecution
from transform.facts.fact_target_snapshot import FactTargetSnapshot
from transform.facts.fact_project_progress import FactProjectProgress

__all__ = [
    "BaseFact",
    "ScenarioEngine",
    "FactTaskExecution",
    "FactTargetSnapshot",
    "FactProjectProgress",
]
