import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.marts.base_mart import BaseMart

logger = logging.getLogger(__name__)


class MartTaskExecution(BaseMart):
    """Semantic Mart 3: Phân tích Thực thi Công việc & Điểm nghẽn WBS (Task Execution & Bottlenecks).
    
    Phục vụ Team Lead & Trưởng dự án:
      - Tự động phát hiện điểm nghẽn nghiêm trọng (`bottleneck_flag` = BLOCKING/DELAYED).
      - Tính điểm cấp bách tổng hợp (`urgency_score`).
      - Cảnh báo tình trạng quá tải của người nhận việc (`assignee_workload_flag`).
      - Xếp hạng mức độ trễ hạn theo phòng ban và theo dự án.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/marts"):
        super().__init__(spark_session, mart_name="mart_task_execution", output_base=output_base)

    def extract(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any]):
        return {
            "dim_task": dim_tables.get("dim_task"),
            "dim_project": dim_tables.get("dim_project"),
            "dim_resource": dim_tables.get("dim_resource"),
            "dim_department": dim_tables.get("dim_department"),
            "fact_task_execution": fact_tables.get("fact_task_execution")
        }

    def build_mart(self, data: Dict[str, Any]):
        dim_t = data.get("dim_task")
        dim_p = data.get("dim_project")
        fact_t = data.get("fact_task_execution")
        dim_r = data.get("dim_resource")
        dim_d = data.get("dim_department")

        if fact_t is None and dim_t is None:
            logger.warning("Không có dữ liệu tasks để build mart_task_execution")
            return None

        # 1. Base join fact_task_execution với dim_task
        if fact_t is not None and dim_t is not None:
            base = fact_t.join(
                dim_t.select("task_id", "task_name", "task_type", "description", "state", "track_status", "parent_task_id"),
                "task_id",
                "left"
            )
        elif fact_t is not None:
            base = fact_t.withColumn("task_name", F.col("task_id"))
        else:
            base = dim_t.withColumn("snapshot_date", F.current_date()).withColumn(
                "percent_completed", F.lit(0.0)
            ).withColumn(
                "is_overdue", F.lit(False)
            ).withColumn(
                "days_overdue", F.lit(0)
            ).withColumn(
                "duration_variance", F.lit(0.0)
            ).withColumn(
                "effort_variance", F.lit(0.0)
            )

        # 2. Join thông tin Dự án
        if dim_p is not None:
            curr_p = dim_p.filter(F.col("is_current") == True) if "is_current" in dim_p.columns else dim_p
            base = base.join(
                curr_p.select("project_id", "project_name", "department_id"),
                "project_id",
                "left"
            )
        else:
            base = base.withColumn("project_name", F.col("project_id")).withColumn("department_id", F.lit("UNKNOWN"))

        # 3. Join Tên Nhân sự
        if dim_r is not None and "resource_id" in dim_r.columns:
            base = base.join(
                dim_r.select(F.col("resource_id").alias("assignee_id"), F.col("resource_name").alias("assignee_name")),
                "assignee_id",
                "left"
            )
        else:
            base = base.withColumn("assignee_name", F.col("assignee_id"))

        # 4. Join Tên Phòng ban
        if dim_d is not None and "department_id" in dim_d.columns:
            base = base.join(
                dim_d.select("department_id", "department_name"),
                "department_id",
                "left"
            )
        else:
            base = base.withColumn("department_name", F.col("department_id"))

        # 5. Điền giá trị rỗng
        filled = base.fillna({
            "task_name": "Task", "project_name": "Project", "assignee_name": "Chưa phân công",
            "department_name": "Chưa xác định", "priority": 50.0, "days_overdue": 0, "is_overdue": False
        })

        # 6. Pre-computed Intelligence: Phát hiện Điểm nghẽn & Độ cấp bách
        with_flags = filled.withColumn(
            # CỜ ĐIỂM NGHẼN (Bottleneck Flag): BLOCKING (trễ và <30%) / DELAYED / ON_TRACK
            "bottleneck_flag",
            F.when(
                (F.col("is_overdue") == True) & (F.col("percent_completed") < 30.0),
                F.lit("BLOCKING")
            ).when(
                (F.col("is_overdue") == True) | (F.col("duration_variance") > 0.0),
                F.lit("DELAYED")
            ).otherwise(F.lit("ON_TRACK"))
        ).withColumn(
            # ĐIỂM CẤP BÁCH: Priority * (1 + days_overdue / 7)
            "urgency_score",
            F.round(F.col("priority") * (F.lit(1.0) + (F.greatest(F.col("days_overdue"), F.lit(0)) / F.lit(7.0))), 2)
        )

        # 7. Window Functions: Tải công việc của nhân sự & Thứ hạng
        assignee_window = Window.partitionBy("assignee_id", "snapshot_date")
        proj_rank_window = Window.partitionBy("project_id").orderBy(F.col("urgency_score").desc())
        dept_overdue_window = Window.partitionBy("department_id").orderBy(F.col("days_overdue").desc())

        final_df = with_flags.withColumn(
            # Số task đồng thời của cùng assignee
            "assignee_concurrent_tasks", F.count(F.lit(1)).over(assignee_window)
        ).withColumn(
            # Số task trễ của cùng assignee
            "assignee_overdue_count", F.sum(F.when(F.col("is_overdue") == True, 1).otherwise(0)).over(assignee_window)
        ).withColumn(
            # CỜ TẢI NHÂN SỰ: OVERLOADED (>5 tasks active) / NORMAL / LIGHT
            "assignee_workload_flag",
            F.when(F.col("assignee_concurrent_tasks") > 5, F.lit("OVERLOADED"))
             .when(F.col("assignee_concurrent_tasks") >= 2, F.lit("NORMAL"))
             .otherwise(F.lit("LIGHT"))
        ).withColumn(
            # Thứ hạng ưu tiên task trong dự án
            "project_task_rank", F.rank().over(proj_rank_window)
        ).withColumn(
            # Thứ hạng trễ hạn trong phòng ban
            "dept_overdue_rank", F.rank().over(dept_overdue_window)
        ).withColumn(
            "_materialized_at", F.current_timestamp()
        )

        logger.info(json.dumps({
            "event": "mart_task_execution_built",
            "mart": self.mart_name
        }))
        return final_df
