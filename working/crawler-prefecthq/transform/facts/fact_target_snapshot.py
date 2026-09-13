import logging
import json
from typing import Dict, Any, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.facts.base_fact import BaseFact
from transform.facts.scenario_engine import ScenarioEngine

logger = logging.getLogger(__name__)


class FactTargetSnapshot(BaseFact):
    """Bảng Fact theo dõi ảnh hưởng và kết quả mục tiêu đa kịch bản (Multi-scenario Target Snapshot).
    
    Grain: 1 dòng cho mỗi Target / Scenario (M, N, S) / Ngày Snapshot.
    Sử dụng ScenarioEngine để unpivot và tính toán các chỉ số độ lệch, hoàn thành và cảnh báo rủi ro.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/facts"):
        super().__init__(spark_session, fact_name="fact_target_snapshot", output_base=output_base)
        self.engine = ScenarioEngine()

    def extract(self, bronze_sources: Dict[str, Any], dim_sources: Optional[Dict[str, Any]] = None):
        """Trích xuất từ bronze targets."""
        df = bronze_sources.get("targets") or bronze_sources.get("target")
        if df is None:
            logger.warning("Không tìm thấy bronze DataFrame cho targets")
            return None
        return df

    def compute_measures(self, df):
        """Thực hiện unpivot 3 kịch bản M/N/S và bổ sung các chỉ số phân tích chuyên sâu."""
        if df is None:
            return None

        snapshot_date_col = "ingest_date" if "ingest_date" in df.columns else "snapshot_date"
        snap_expr = F.col(snapshot_date_col) if snapshot_date_col in df.columns else F.current_date()

        # 1. Danh sách các cột chung cần giữ lại khi unpivot
        common_cols = [
            "sysid", "name", "target_type", "unit", "associated_objective",
            "c_associated_assignment", "associated_item", "c_assignee",
            "c_department", "weight", "percent_completed", "state", "status"
        ]

        with_snap = df.withColumn("snapshot_date", snap_expr)
        common_cols.append("snapshot_date")

        # 2. Sử dụng ScenarioEngine để unpivot M/N/S và tính Gap Analysis
        unpivoted = self.engine.unpivot(with_snap, common_cols)
        analyzed = self.engine.compute_gap_analysis(unpivoted, snapshot_date_col="snapshot_date")

        # 3. Chuẩn hóa tên khóa ngoại và thuộc tính
        standardized = analyzed.select(
            F.col("sysid").alias("target_id"),
            F.col("name").alias("target_name"),
            F.coalesce(F.col("associated_objective"), F.lit("UNKNOWN")).alias("objective_id"),
            F.coalesce(F.col("c_associated_assignment"), F.lit("UNKNOWN")).alias("assignment_id"),
            F.coalesce(F.col("associated_item"), F.lit("UNKNOWN")).alias("project_id"),
            F.coalesce(F.col("c_assignee"), F.lit("UNKNOWN")).alias("assignee_id"),
            F.coalesce(F.col("c_department"), F.lit("UNKNOWN")).alias("department_id"),
            F.col("scenario"),
            F.col("target_type"),
            F.col("unit"),
            F.col("target_date"),
            F.col("snapshot_date"),
            # Measures & Derived Metrics
            F.col("target_value"),
            F.col("target_result"),
            F.col("achievement_pct"),
            F.col("gap_value"),
            F.col("gap_pct"),
            F.col("is_achieved"),
            F.col("days_to_deadline"),
            F.col("is_at_risk"),
            F.col("weighted_achievement"),
            F.col("weight"),
            F.col("percent_completed"),
            F.col("state"),
            F.col("status")
        )

        # 4. Chuyển đổi Date Keys
        with_keys = self.add_date_key(standardized, "target_date", "target_date_key")
        with_keys = self.add_date_key(with_keys, "snapshot_date", "snapshot_date_key")

        # 5. Gán Surrogate Key (target_snapshot_sk)
        window_spec = Window.orderBy("target_id", "scenario", "snapshot_date")
        final_df = with_keys.withColumn(
            "target_snapshot_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "fact_target_snapshot_computed",
            "fact": self.fact_name
        }))
        return final_df
