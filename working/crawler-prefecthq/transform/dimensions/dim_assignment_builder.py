import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimAssignmentBuilder(BaseDimension):
    """Xây dựng bảng dim_assignment (Phân công giao việc).
    
    Quản lý thông tin phân công, cấp phân công cha-con (c_parent_assignment),
    và các trọng số hoàn thành phục vụ đánh giá năng lực thực thi.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_assignment", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any]):
        """Trích xuất từ bronze c_assignments."""
        df = (
            bronze_sources.get("c_assignments")
            or bronze_sources.get("c_assignment")
            or bronze_sources.get("assignments")
        )
        if df is None:
            logger.warning("Không tìm thấy bronze DataFrame cho c_assignments")
            return None
        return df

    def transform(self, df):
        """Ánh xạ thuộc tính phân công và gán Surrogate Key (assignment_sk)."""
        if df is None:
            return None

        mapped = df.select(
            F.col("sysid").alias("assignment_id"),
            F.col("name").alias("assignment_name"),
            F.col("description"),
            F.coalesce(F.col("c_department"), F.lit("UNKNOWN")).alias("department_id"),
            F.coalesce(F.col("c_assignee"), F.col("entity_owner"), F.lit("UNKNOWN")).alias("assignee_id"),
            F.coalesce(F.col("c_assignor"), F.col("created_by"), F.lit("UNKNOWN")).alias("assignor_id"),
            F.col("c_parent_assignment").alias("parent_assignment_id"),
            F.col("c_start_date").alias("start_date"),
            F.col("c_end_date").alias("end_date"),
            F.coalesce(F.col("c_achievement_rate"), F.lit(0.0)).alias("achievement_rate"),
            F.coalesce(F.col("c_total_weight"), F.lit(1.0)).alias("total_weight"),
            F.coalesce(F.col("c_sum_target_weight_percent"), F.lit(0.0)).alias("sum_target_weight_percent"),
            F.coalesce(F.col("created_on"), F.col("last_updated_on"), F.current_date()).alias("created_on"),
            F.coalesce(F.col("last_updated_on"), F.current_date()).alias("last_updated_on")
        )

        window_spec = Window.orderBy("assignment_id")
        final_df = mapped.withColumn(
            "assignment_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "dim_assignment_transformed",
            "dimension": self.dim_name
        }))
        return final_df
