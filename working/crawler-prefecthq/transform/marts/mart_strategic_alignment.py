import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.marts.base_mart import BaseMart

logger = logging.getLogger(__name__)


class MartStrategicAlignment(BaseMart):
    """Semantic Mart 1: Phân tích Liên kết Chiến lược (BSC -> Project -> Target).
    
    Phục vụ Lãnh đạo & PMO:
      - Đánh giá mức độ lan tỏa từ mục tiêu cấp cao xuống các dự án và mục tiêu chi tiết.
      - Tính toán sẵn tỷ lệ bao phủ liên kết (`alignment_coverage_pct`), điểm trọng số BSC,
        phân hạng nội bộ phòng ban (`dept_rank`), và cờ sức khỏe chiến lược (`objective_health`).
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/marts"):
        super().__init__(spark_session, mart_name="mart_strategic_alignment", output_base=output_base)

    def extract(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any]):
        return {
            "dim_objective": dim_tables.get("dim_objective"),
            "dim_project": dim_tables.get("dim_project"),
            "dim_department": dim_tables.get("dim_department"),
            "dim_resource": dim_tables.get("dim_resource"),
            "fact_target_snapshot": fact_tables.get("fact_target_snapshot")
        }

    def build_mart(self, data: Dict[str, Any]):
        dim_obj = data.get("dim_objective")
        dim_proj = data.get("dim_project")
        fact_tgt = data.get("fact_target_snapshot")
        dim_dept = data.get("dim_department")
        dim_res = data.get("dim_resource")

        if dim_obj is None:
            logger.warning("dim_objective không tồn tại để build mart_strategic_alignment")
            return None

        # 1. Tổng hợp liên kết từ bảng Dự án theo objective_id
        if dim_proj is not None and "associated_objective_id" in dim_proj.columns:
            # Nếu dim_project là SCD 2, chỉ lấy bản ghi hiện hành (is_current == True)
            curr_proj = dim_proj.filter(F.col("is_current") == True) if "is_current" in dim_proj.columns else dim_proj
            proj_agg = curr_proj.filter(
                F.col("associated_objective_id").isNotNull() & (F.col("associated_objective_id") != "UNKNOWN")
            ).groupBy("associated_objective_id").agg(
                F.countDistinct("project_id").alias("linked_projects_count"),
                F.countDistinct(F.when(F.col("track_status") == "OnTrack", F.col("project_id"))).alias("linked_projects_on_track"),
                F.countDistinct(F.when(F.col("track_status").isin("AtRisk", "OffTrack"), F.col("project_id"))).alias("linked_projects_at_risk")
            )
        else:
            proj_agg = dim_obj.select(F.col("objective_id").alias("associated_objective_id")).withColumn(
                "linked_projects_count", F.lit(0)
            ).withColumn(
                "linked_projects_on_track", F.lit(0)
            ).withColumn(
                "linked_projects_at_risk", F.lit(0)
            )

        # 2. Tổng hợp kết quả Target theo objective_id
        if fact_tgt is not None and "objective_id" in fact_tgt.columns:
            tgt_agg = fact_tgt.groupBy("objective_id").agg(
                F.countDistinct("target_id").alias("total_targets"),
                F.countDistinct(F.when((F.col("scenario") == "M") & (F.col("is_achieved") == True), F.col("target_id"))).alias("achieved_targets_m"),
                F.countDistinct(F.when((F.col("scenario") == "N") & (F.col("is_achieved") == True), F.col("target_id"))).alias("achieved_targets_n")
            )
        else:
            tgt_agg = dim_obj.select("objective_id").withColumn(
                "total_targets", F.lit(0)
            ).withColumn(
                "achieved_targets_m", F.lit(0)
            ).withColumn(
                "achieved_targets_n", F.lit(0)
            )

        # 3. Join với dim_objective
        joined = dim_obj.join(
            proj_agg,
            dim_obj["objective_id"] == proj_agg["associated_objective_id"],
            "left"
        ).join(
            tgt_agg,
            "objective_id",
            "left"
        )

        # 4. Bổ sung tên phòng ban & người phụ trách
        if dim_dept is not None and "department_id" in dim_dept.columns:
            joined = joined.join(
                dim_dept.select("department_id", "department_name"),
                "department_id",
                "left"
            )
        else:
            joined = joined.withColumn("department_name", F.col("department_id"))

        if dim_res is not None and "resource_id" in dim_res.columns:
            joined = joined.join(
                dim_res.select(F.col("resource_id").alias("assignee_id"), F.col("resource_name").alias("assignee_name")),
                "assignee_id",
                "left"
            )
        else:
            joined = joined.withColumn("assignee_name", F.col("assignee_id"))

        # 5. Điền giá trị mặc định cho NULL
        filled = joined.fillna({
            "linked_projects_count": 0, "linked_projects_on_track": 0, "linked_projects_at_risk": 0,
            "total_targets": 0, "achieved_targets_m": 0, "achieved_targets_n": 0, "weight": 1.0,
            "department_name": "Chưa xác định", "assignee_name": "Chưa phân công"
        })

        # 6. Tính toán Pre-computed Intelligence
        with_metrics = filled.withColumn(
            # Tỷ lệ đạt Target kịch bản M
            "target_achievement_rate_m",
            F.when(
                F.col("total_targets") > 0,
                F.round((F.col("achieved_targets_m") / F.col("total_targets")) * 100.0, 2)
            ).otherwise(F.lit(100.0))
        ).withColumn(
            # Tỷ lệ đạt Target kịch bản N
            "target_achievement_rate_n",
            F.when(
                F.col("total_targets") > 0,
                F.round((F.col("achieved_targets_n") / F.col("total_targets")) * 100.0, 2)
            ).otherwise(F.lit(100.0))
        ).withColumn(
            # Điểm trọng số BSC
            "weighted_score",
            F.round(F.col("target_achievement_rate_m") * F.coalesce(F.col("weight"), F.lit(1.0)), 2)
        ).withColumn(
            # Cờ sức khỏe mục tiêu (Objective Health)
            "objective_health",
            F.when(F.col("target_achievement_rate_m") >= 100.0, F.lit("ACHIEVED"))
             .when(F.col("target_achievement_rate_m") >= 80.0, F.lit("ON_TRACK"))
             .when(F.col("target_achievement_rate_m") >= 50.0, F.lit("AT_RISK"))
             .otherwise(F.lit("CRITICAL"))
        )

        # 7. Phân hạng (Ranking) & Đối chuẩn trung bình phòng ban (Benchmarking)
        dept_window = Window.partitionBy("department_id").orderBy(F.col("weighted_score").desc())
        dept_avg_window = Window.partitionBy("department_id")

        final_df = with_metrics.withColumn(
            "dept_rank", F.rank().over(dept_window)
        ).withColumn(
            "vs_dept_avg_achievement",
            F.round(F.col("weighted_score") - F.avg("weighted_score").over(dept_avg_window), 2)
        ).withColumn(
            "_materialized_at", F.current_timestamp()
        )

        logger.info(json.dumps({
            "event": "mart_strategic_alignment_built",
            "mart": self.mart_name
        }))
        return final_df
