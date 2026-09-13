"""Package chứa 5 Semantic Mart Builders (Materialized Views) cho EPM Data Warehouse.

Các Marts gồm:
  - mart_strategic_alignment: Phân tích liên kết chiến lược (BSC -> Project -> Target)
  - mart_project_health: Giám sát sức khỏe danh mục dự án và xu hướng
  - mart_task_execution: Phân tích thực thi công việc và điểm nghẽn WBS
  - mart_resource_allocation: Ma trận phân bổ nguồn lực và mức độ quá tải
  - mart_kpi_gap_analysis: Phân tích khoảng cách đạt chỉ tiêu theo kịch bản M/N/S
"""

from transform.marts.base_mart import BaseMart
from transform.marts.mart_strategic_alignment import MartStrategicAlignment
from transform.marts.mart_project_health import MartProjectHealth
from transform.marts.mart_task_execution import MartTaskExecution
from transform.marts.mart_resource_allocation import MartResourceAllocation
from transform.marts.mart_kpi_gap_analysis import MartKPIGapAnalysis

__all__ = [
    "BaseMart",
    "MartStrategicAlignment",
    "MartProjectHealth",
    "MartTaskExecution",
    "MartResourceAllocation",
    "MartKPIGapAnalysis",
]
