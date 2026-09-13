import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimTaskBuilder(BaseDimension):
    """Xây dựng bảng dim_task (Dimension công việc trong WBS).
    
    Lưu trữ thông tin thuộc tính của Task (SCD Type 1). Các thông số đo lường
    tiến độ biến thiên liên tục (Work, Duration, ActualEffort, % Completed)
    sẽ được tách ra Fact table (fact_task_execution) theo mô hình Periodic Snapshot.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_task", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any]):
        """Trích xuất từ bronze tasks."""
        df = bronze_sources.get("tasks") or bronze_sources.get("task")
        if df is None:
            logger.warning("Không tìm thấy bronze DataFrame cho tasks")
            return None
        return df

    def transform(self, df):
        """Ánh xạ thuộc tính Task và tạo Surrogate Key (task_sk)."""
        if df is None:
            return None

        mapped = df.select(
            F.col("sysid").alias("task_id"),
            F.col("name").alias("task_name"),
            F.coalesce(F.col("task_type"), F.lit("Standard")).alias("task_type"),
            F.col("description"),
            F.coalesce(F.col("state"), F.lit("Active")).alias("state"),
            F.coalesce(F.col("track_status"), F.lit("Unknown")).alias("track_status"),
            F.coalesce(F.col("priority"), F.lit(50.0)).alias("priority"),
            F.coalesce(F.col("parent_project"), F.col("project"), F.lit("UNKNOWN")).alias("project_id"),
            F.col("parent").alias("parent_task_id"),
            F.coalesce(F.col("entity_owner"), F.lit("UNKNOWN")).alias("assignee_id"),
            F.coalesce(F.col("created_by"), F.lit("UNKNOWN")).alias("assignor_id"),
            F.col("external_id"),
            F.col("start_date"),
            F.col("due_date"),
            F.coalesce(F.col("created_on"), F.col("last_updated_on"), F.current_date()).alias("created_on"),
            F.coalesce(F.col("last_updated_on"), F.current_date()).alias("last_updated_on")
        )

        window_spec = Window.orderBy("task_id")
        final_df = mapped.withColumn(
            "task_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "dim_task_transformed",
            "dimension": self.dim_name
        }))
        return final_df
