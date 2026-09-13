import logging
import json
from typing import Dict, Any, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.facts.base_fact import BaseFact

logger = logging.getLogger(__name__)


class FactProjectProgress(BaseFact):
    """Bảng Fact tổng hợp tiến độ và sức khỏe dự án định kỳ (Project Progress Snapshot).
    
    Grain: 1 dòng cho mỗi Dự án tại mỗi Ngày Snapshot.
    Tổng hợp trực tiếp các số liệu cấp thấp từ task_execution và target_snapshot
    để tính toán chỉ số sức khỏe tổng hợp đa chiều `health_score`.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/facts"):
        super().__init__(spark_session, fact_name="fact_project_progress", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any], dim_sources: Optional[Dict[str, Any]] = None):
        """Trích xuất từ bronze projects kèm theo các fact phụ thuộc (nếu có)."""
        project_df = bronze_sources.get("projects") or bronze_sources.get("project")
        task_fact_df = bronze_sources.get("fact_task_execution")
        target_fact_df = bronze_sources.get("fact_target_snapshot")

        return {
            "projects": project_df,
            "tasks": task_fact_df,
            "targets": target_fact_df
        }

    def compute_measures(self, extracted: Dict[str, Any]):
        """Tính toán tổng hợp tiến độ công việc, đạt chỉ tiêu, và composite health_score."""
        p_df = extracted.get("projects")
        t_df = extracted.get("tasks")
        target_df = extracted.get("targets")

        if p_df is None:
            logger.warning("Không tìm thấy dữ liệu projects để tính fact_project_progress")
            return None

        snapshot_date_col = "ingest_date" if "ingest_date" in p_df.columns else "snapshot_date"
        snap_expr = F.col(snapshot_date_col) if snapshot_date_col in p_df.columns else F.current_date()

        # 1. Chuẩn bị bảng gốc dự án
        base_proj = p_df.select(
            F.col("sysid").alias("project_id"),
            F.coalesce(F.col("c_department"), F.lit("UNKNOWN")).alias("department_id"),
            F.coalesce(F.col("project_manager"), F.col("manager"), F.lit("UNKNOWN")).alias("project_manager_id"),
            F.coalesce(F.col("percent_completed"), F.lit(0.0)).alias("percent_completed"),
            F.coalesce(F.col("track_status"), F.lit("Unknown")).alias("track_status"),
            F.coalesce(F.col("state"), F.lit("Active")).alias("state"),
            snap_expr.alias("snapshot_date")
        )

        # 2. Tổng hợp số liệu từ Tasks (nếu có)
        if t_df is not None and "project_id" in t_df.columns:
            task_agg = t_df.groupBy("project_id").agg(
                F.count(F.lit(1)).alias("total_tasks"),
                F.sum(F.when(F.col("percent_completed") >= 100.0, 1).otherwise(0)).alias("completed_tasks"),
                F.sum(F.when(F.col("is_overdue") == True, 1).otherwise(0)).alias("overdue_tasks"),
                F.round(F.avg("percent_completed"), 2).alias("avg_task_completion"),
                F.round(F.sum("work_planned"), 2).alias("total_work_planned"),
                F.round(F.sum("actual_effort"), 2).alias("total_work_actual")
            )
        else:
            task_agg = base_proj.select("project_id").withColumn(
                "total_tasks", F.lit(0)
            ).withColumn(
                "completed_tasks", F.lit(0)
            ).withColumn(
                "overdue_tasks", F.lit(0)
            ).withColumn(
                "avg_task_completion", F.lit(0.0)
            ).withColumn(
                "total_work_planned", F.lit(0.0)
            ).withColumn(
                "total_work_actual", F.lit(0.0)
            )

        # 3. Tổng hợp số liệu từ Targets (nếu có)
        if target_df is not None and "project_id" in target_df.columns:
            target_agg = target_df.groupBy("project_id").agg(
                F.countDistinct("target_id").alias("total_targets"),
                F.sum(F.when((F.col("scenario") == "M") & (F.col("is_achieved") == True), 1).otherwise(0)).alias("achieved_targets_m"),
                F.sum(F.when((F.col("scenario") == "N") & (F.col("is_achieved") == True), 1).otherwise(0)).alias("achieved_targets_n")
            )
        else:
            target_agg = base_proj.select("project_id").withColumn(
                "total_targets", F.lit(0)
            ).withColumn(
                "achieved_targets_m", F.lit(0)
            ).withColumn(
                "achieved_targets_n", F.lit(0)
            )

        # 4. Join tổng hợp vào dự án
        joined = base_proj.join(task_agg, "project_id", "left").join(target_agg, "project_id", "left")

        # 5. Điền giá trị mặc định cho các cột tính toán
        filled = joined.fillna({
            "total_tasks": 0, "completed_tasks": 0, "overdue_tasks": 0,
            "avg_task_completion": 0.0, "total_work_planned": 0.0, "total_work_actual": 0.0,
            "total_targets": 0, "achieved_targets_m": 0, "achieved_targets_n": 0
        })

        # 6. Tính toán các chỉ số sức khỏe chuyên sâu (Pre-computed Health Indicators)
        with_health = filled.withColumn(
            # Tỷ lệ task trễ hạn
            "task_overdue_ratio",
            F.when(
                F.col("total_tasks") > 0,
                F.round(F.col("overdue_tasks") / F.col("total_tasks"), 4)
            ).otherwise(F.lit(0.0))
        ).withColumn(
            # Hiệu quả sử dụng công sức (planned / actual, >1.0 là tối ưu)
            "work_efficiency",
            F.when(
                F.col("total_work_actual") > 0,
                F.round(F.col("total_work_planned") / F.col("total_work_actual"), 2)
            ).otherwise(F.lit(1.0))
        ).withColumn(
            # Tỷ lệ đạt mục tiêu kịch bản M
            "target_achievement_rate",
            F.when(
                F.col("total_targets") > 0,
                F.round((F.col("achieved_targets_m") / F.col("total_targets")) * 100.0, 2)
            ).otherwise(F.lit(100.0))
        ).withColumn(
            # Chuẩn hóa work_efficiency (cap ở 1.5 để tránh outlier inflate điểm)
            "work_efficiency_capped",
            F.least(F.col("work_efficiency"), F.lit(1.5)) * 66.67
        ).withColumn(
            # COMPOSITE HEALTH SCORE: 40% Tiến độ + 25% Không trễ task + 20% Đạt Target + 15% Hiệu quả công sức
            "health_score",
            F.round(
                (F.col("percent_completed") * 0.40) +
                ((F.lit(1.0) - F.col("task_overdue_ratio")) * 100.0 * 0.25) +
                (F.col("target_achievement_rate") * 0.20) +
                (F.col("work_efficiency_capped") * 0.15),
                2
            )
        ).withColumn(
            # Cờ cảnh báo dự án có rủi ro cao (Health Score < 50 hoặc tỷ lệ task trễ > 30%)
            "is_at_risk",
            F.when(
                (F.col("health_score") < 50.0) | (F.col("task_overdue_ratio") > 0.30) | (F.col("track_status") == "OffTrack"),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).drop("work_efficiency_capped")

        # 7. Chuyển đổi Date Key
        with_keys = self.add_date_key(with_health, "snapshot_date", "snapshot_date_key")

        # 8. Gán Surrogate Key (project_progress_sk)
        window_spec = Window.orderBy("project_id", "snapshot_date")
        final_df = with_keys.withColumn(
            "project_progress_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "fact_project_progress_computed",
            "fact": self.fact_name
        }))
        return final_df
