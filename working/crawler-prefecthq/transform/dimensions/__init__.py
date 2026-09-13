"""Package chứa các module xây dựng 7 bảng Dimension Kimball Star Schema.

Các Dimension gồm:
  - dim_date: Trục thời gian 2020-2030 (Date Spine)
  - dim_department: Danh mục phòng ban trích xuất chéo (Cross-table Union)
  - dim_resource: Danh mục nhân sự/nguồn lực trích xuất chéo
  - dim_project: Dự án hỗ trợ SCD Type 2 (Time-travel & GenBI Query)
  - dim_task: Công việc phân cấp WBS
  - dim_objective: Mục tiêu chiến lược BSC kèm Hierarchy Flattening
  - dim_assignment: Phân công giao việc
"""

from transform.dimensions.base_dimension import BaseDimension
from transform.dimensions.dim_date_generator import DimDateGenerator
from transform.dimensions.dim_department_builder import DimDepartmentBuilder
from transform.dimensions.dim_resource_builder import DimResourceBuilder
from transform.dimensions.dim_project_builder import DimProjectBuilder
from transform.dimensions.dim_task_builder import DimTaskBuilder
from transform.dimensions.dim_objective_builder import DimObjectiveBuilder
from transform.dimensions.dim_assignment_builder import DimAssignmentBuilder

__all__ = [
    "BaseDimension",
    "DimDateGenerator",
    "DimDepartmentBuilder",
    "DimResourceBuilder",
    "DimProjectBuilder",
    "DimTaskBuilder",
    "DimObjectiveBuilder",
    "DimAssignmentBuilder",
]
