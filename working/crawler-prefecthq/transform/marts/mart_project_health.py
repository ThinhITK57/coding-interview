import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.marts.base_mart import BaseMart

logger = logging.getLogger(__name__)


class MartProjectHealth(BaseMart):
    """Semantic Mart 2: Giám sát Sức khỏe & Tiến độ Danh mục Dự án (Project Health & Delivery).
    
    Phục vụ Ban Giám đốc & PMO:
      - Cung cấp cái nhìn tức thì về sức khỏe của từng dự án (`health_score`).
      - Tính toán sẵn xu hướng biến động (`health_trend`), bách phân vị toàn công ty (`portfolio_percentile`),
        thứ hạng trong phòng ban (`dept_health_rank`), và cờ dự án không được cập nhật (`is_stale`).
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/marts"):
        super().__init__(spark_session, mart_name="mart_project_health", output_base=output_base)

    def extract(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any]):
        return {
            "dim_project": dim_tables.get("dim_project"),
            "dim_department": dim_tables.get("dim_department"),
            "dim_resource": dim_tables.get("dim_resource"),
            "fact_project_progress": fact_tables.get("fact_project_progress")
        }

    def build_mart(self, data: Dict[str, Any]):
        dim_proj = data.get("dim_project")
        fact_prog = data.get("fact_project_progress")
        dim_dept = data.get("dim_department")
        dim_res = data.get("dim_resource")

        if fact_prog is None and dim_proj is None:
            logger.warning("Không có dữ liệu để build mart_project_health")
            return None

        # 1. Lọc bảng dự án hiện hành nếu là SCD 2
        curr_proj = dim_proj
        if dim_proj is not None and "is_current" in dim_proj.columns:
            curr_proj = dim_proj.filter(F.col("is_current") == True)

        # 2. Base join giữa fact_project_progress và dim_project
        if fact_prog is not None:
            base = fact_prog.join(
                curr_proj.select("project_id", "project_name", "project_type", "created_on", "last_updated_on"),
                "project_id",
                "left"
            )
        else:
            base = curr_proj.withColumn(
                "snapshot_date", F.current_date()
            ).withColumn(
                "health_score", F.col("percent_completed")
            ).withColumn(
                "task_overdue_ratio", F.lit(0.0)
            ).withColumn(
                "work_efficiency", F.lit(1.0)
            ).withColumn(
                "target_achievement_rate", F.lit(100.0)
            ).withColumn(
                "total_tasks", F.lit(0)
            ).withColumn(
                "completed_tasks", F.lit(0)
            ).withColumn(
                "overdue_tasks", F.lit(0)
            )

        # 3. Bổ sung tên phòng ban & PM
        if dim_dept is not None and "department_id" in dim_dept.columns:
            base = base.join(
                dim_dept.select("department_id", "department_name"),
                "department_id",
                "left"
            )
        else:
            base = base.withColumn("department_name", F.col("department_id"))

        if dim_res is not None and "resource_id" in dim_res.columns:
            base = base.join(
                dim_res.select(F.col("resource_id").alias("project_manager_id"), F.col("resource_name").alias("project_manager_name")),
                "project_manager_id",
                "left"
            )
        else:
            base = base.withColumn("project_manager_name", F.col("project_manager_id"))

        # 4. Điền mặc định
        filled = base.fillna({
            "department_name": "Chưa xác định",
            "project_manager_name": "Chưa phân công",
            "health_score": 50.0,
            "percent_completed": 0.0
        })

        # 5. Phân loại sức khỏe (Health Category)
        with_category = filled.withColumn(
            "health_category",
            F.when(F.col("health_score") >= 70.0, F.lit("HEALTHY"))
             .when(F.col("health_score") >= 50.0, F.lit("WARNING"))
             .otherwise(F.lit("CRITICAL"))
        ).withColumn(
            # Số ngày kể từ lần cập nhật cuối
            "days_since_last_update",
            F.when(
                F.col("last_updated_on").isNotNull(),
                F.datediff(F.col("snapshot_date"), F.col("last_updated_on"))
            ).otherwise(F.lit(0))
        ).withColumn(
            # Cờ dự án không cập nhật quá 7 ngày
            "is_stale",
            F.when(F.col("days_since_last_update") > 7, F.lit(True)).otherwise(F.lit(False))
        )

        # 6. Window Functions: Xu hướng (Trend) & Thứ hạng (Ranking & Percentile)
        proj_time_window = Window.partitionBy("project_id").orderBy("snapshot_date")
        dept_window = Window.partitionBy("department_id").orderBy(F.col("health_score").desc())
        dept_avg_window = Window.partitionBy("department_id")
        portfolio_window = Window.orderBy(F.col("health_score").asc())

        final_df = with_category.withColumn(
            # Chênh lệch điểm sức khỏe so với lần snapshot trước
            "health_trend",
            F.round(F.col("health_score") - F.coalesce(F.lag("health_score", 1).over(proj_time_window), F.col("health_score")), 2)
        ).withColumn(
            "health_trend_direction",
            F.when(F.col("health_trend") > 0.0, F.lit("IMPROVING"))
             .when(F.col("health_trend") < 0.0, F.lit("DECLINING"))
             .otherwise(F.lit("STABLE"))
        ).withColumn(
            # Thứ hạng trong phòng ban
            "dept_health_rank", F.rank().over(dept_window)
        ).withColumn(
            # So sánh với điểm trung bình phòng ban
            "vs_dept_avg_health",
            F.round(F.col("health_score") - F.avg("health_score").over(dept_avg_window), 2)
        ).withColumn(
            # Bách phân vị toàn danh mục công ty (0 - 100%)
            "portfolio_percentile",
            F.round(F.percent_rank().over(portfolio_window) * 100.0, 1)
        ).withColumn(
            "_materialized_at", F.current_timestamp()
        )

        logger.info(json.dumps({
            "event": "mart_project_health_built",
            "mart": self.mart_name
        }))
        return final_df
