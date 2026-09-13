import logging
import json
from typing import Dict, Any, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.facts.base_fact import BaseFact

logger = logging.getLogger(__name__)


class FactTaskExecution(BaseFact):
    """Bảng Fact ghi nhận tiến độ thực thi công việc định kỳ (Periodic Snapshot Fact).
    
    Grain: 1 dòng cho mỗi Task tại mỗi ngày chụp ảnh (Snapshot Date).
    Tính toán sẵn các chỉ số phát hiện chậm trễ, vượt thời lượng và hiệu quả effort.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/facts"):
        super().__init__(spark_session, fact_name="fact_task_execution", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any], dim_sources: Optional[Dict[str, Any]] = None):
        """Trích xuất từ bronze tasks."""
        df = bronze_sources.get("tasks") or bronze_sources.get("task")
        if df is None:
            logger.warning("Không tìm thấy bronze DataFrame cho tasks")
            return None
        return df

    def compute_measures(self, df):
        """Tính toán các chỉ số thực thi và chuyển đổi date keys."""
        if df is None:
            return None

        snapshot_date_col = "ingest_date" if "ingest_date" in df.columns else "snapshot_date"
        snap_expr = F.col(snapshot_date_col) if snapshot_date_col in df.columns else F.current_date()

        # 1. Trích xuất thuộc tính cơ bản & số đo thô
        base = df.select(
            F.col("sysid").alias("task_id"),
            F.coalesce(F.col("parent_project"), F.col("project"), F.lit("UNKNOWN")).alias("project_id"),
            F.coalesce(F.col("entity_owner"), F.lit("UNKNOWN")).alias("assignee_id"),
            F.col("start_date"),
            F.col("due_date"),
            F.col("actual_start_date"),
            F.col("actual_end_date"),
            snap_expr.alias("snapshot_date"),
            # Measures
            F.coalesce(F.col("percent_completed"), F.lit(0.0)).alias("percent_completed"),
            F.coalesce(F.col("work"), F.lit(0.0)).alias("work_planned"),
            F.coalesce(F.col("duration"), F.lit(0.0)).alias("duration_planned"),
            F.coalesce(F.col("actual_duration"), F.lit(0.0)).alias("actual_duration"),
            F.coalesce(F.col("actual_effort"), F.lit(0.0)).alias("actual_effort"),
            F.coalesce(F.col("priority"), F.lit(50.0)).alias("priority")
        )

        # 2. Tính toán Derived Measures (Pre-computed Intelligence)
        with_derived = base.withColumn(
            # Cờ trễ hạn: Đã quá DueDate và chưa đạt 100%
            "is_overdue",
            F.when(
                (F.col("due_date").isNotNull()) & (F.col("due_date") < F.col("snapshot_date")) & (F.col("percent_completed") < 100.0),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            # Số ngày trễ hạn thực tế
            "days_overdue",
            F.when(
                F.col("is_overdue"),
                F.greatest(F.lit(0), F.datediff(F.col("snapshot_date"), F.col("due_date")))
            ).otherwise(F.lit(0))
        ).withColumn(
            # Chênh lệch thời lượng (dương = làm lâu hơn kế hoạch)
            "duration_variance",
            F.round(F.col("actual_duration") - F.col("duration_planned"), 2)
        ).withColumn(
            # Chênh lệch công sức (dương = tốn nhiều giờ hơn kế hoạch)
            "effort_variance",
            F.round(F.col("actual_effort") - F.col("work_planned"), 2)
        ).withColumn(
            # Tỷ lệ tiêu hao công sức vs kế hoạch
            "effort_burn_rate",
            F.when(
                F.col("work_planned") > 0,
                F.round((F.col("actual_effort") / F.col("work_planned")) * 100.0, 2)
            ).otherwise(F.lit(0.0))
        )

        # 3. Chuyển đổi Date Keys sang Integer YYYYMMDD
        with_keys = self.add_date_key(with_derived, "start_date", "start_date_key")
        with_keys = self.add_date_key(with_keys, "due_date", "due_date_key")
        with_keys = self.add_date_key(with_keys, "actual_start_date", "actual_start_date_key")
        with_keys = self.add_date_key(with_keys, "actual_end_date", "actual_end_date_key")
        with_keys = self.add_date_key(with_keys, "snapshot_date", "snapshot_date_key")

        # 4. Gán Surrogate Key (task_execution_sk)
        window_spec = Window.orderBy("task_id", "snapshot_date")
        final_df = with_keys.withColumn(
            "task_execution_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "fact_task_execution_computed",
            "fact": self.fact_name
        }))
        return final_df
