import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimObjectiveBuilder(BaseDimension):
    """Xây dựng bảng dim_objective (Mục tiêu BSC) kèm Hierarchy Flattening.
    
    Tự động tính toán cấp bậc (level) và mục tiêu gốc cao nhất (root_objective_id)
    phục vụ cho use case phân tích chiến lược Cascading Alignment.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_objective", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any]):
        """Trích xuất từ bronze bsc (hoặc objective)."""
        df = bronze_sources.get("bsc") or bronze_sources.get("objective") or bronze_sources.get("objectives")
        if df is None:
            logger.warning("Không tìm thấy bronze DataFrame cho bsc")
            return None
        return df

    def transform(self, df):
        """Ánh xạ thuộc tính BSC, làm phẳng cây phân cấp (level 1-5), và gán Surrogate Key."""
        if df is None:
            return None

        base = df.select(
            F.col("sysid").alias("objective_id"),
            F.col("name").alias("objective_name"),
            F.col("description"),
            F.coalesce(F.col("c_objective_type"), F.lit("Standard")).alias("objective_type"),
            F.coalesce(F.col("state"), F.lit("Active")).alias("state"),
            F.coalesce(F.col("status"), F.lit("Normal")).alias("status"),
            F.coalesce(F.col("c_department"), F.lit("UNKNOWN")).alias("department_id"),
            F.coalesce(F.col("c_assignee"), F.col("entity_owner"), F.lit("UNKNOWN")).alias("assignee_id"),
            F.coalesce(F.col("created_by"), F.lit("UNKNOWN")).alias("assignor_id"),
            F.col("parent_objective").alias("parent_objective_id"),
            F.col("start_date"),
            F.col("end_date"),
            F.coalesce(F.col("weight"), F.lit(1.0)).alias("weight"),
            F.coalesce(F.col("created_on"), F.col("last_updated_on"), F.current_date()).alias("created_on"),
            F.coalesce(F.col("last_updated_on"), F.current_date()).alias("last_updated_on")
        )

        # Hierarchy flattening: Xác định level (1: Top-level objective, 2: Sub-objective, ...)
        # Sử dụng self-join có kiểm soát (phù hợp Spark 2.3.2)
        p1 = base.alias("o").join(
            base.alias("p"),
            F.col("o.parent_objective_id") == F.col("p.objective_id"),
            "left"
        ).select(
            F.col("o.*"),
            F.when(F.col("o.parent_objective_id").isNull() | (F.col("o.parent_objective_id") == ""), F.lit(1))
             .otherwise(F.lit(2)).alias("hierarchy_level"),
            F.coalesce(F.col("p.parent_objective_id"), F.col("o.parent_objective_id"), F.col("o.objective_id")).alias("root_objective_id")
        )

        window_spec = Window.orderBy("objective_id")
        final_df = p1.withColumn(
            "objective_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "dim_objective_transformed",
            "dimension": self.dim_name
        }))
        return final_df
