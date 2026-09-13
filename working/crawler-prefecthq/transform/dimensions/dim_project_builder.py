import logging
import json
from typing import Dict, Any, Optional, List

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimProjectBuilder(BaseDimension):
    """Xây dựng bảng dim_project với chuẩn SCD Type 2 (Slowly Changing Dimension).
    
    Phục vụ tối ưu cho GenBI và phân tích lịch sử theo mốc thời gian:
      - Khi dự án đổi Project Manager (PM), đổi Phòng ban (Department), đổi Trạng thái (TrackStatus),
        hoặc đổi Mục tiêu BSC liên kết (AssociatedObjective), hệ thống sẽ đóng phiên bản cũ và
        mở phiên bản mới với `valid_from`, `valid_to`, `is_current = True`.
      - Cho phép GenBI trả lời các câu hỏi truy vết:
        "Trong khoảng thời gian từ ngày A đến ngày B, ai là PM của dự án X và gắn với mục tiêu nào?"
    """

    # Các thuộc tính trọng yếu kích hoạt sinh phiên bản mới (SCD 2 Triggers)
    SCD2_MONITORED_COLUMNS = [
        "project_manager_id",
        "department_id",
        "track_status",
        "state",
        "associated_objective_id"
    ]

    EXPIRY_DATE_STR = "9999-12-31"

    def __init__(self, spark_session, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_project", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any]):
        """Trích xuất dữ liệu dự án từ bronze projects (đã pruned)."""
        df = bronze_sources.get("projects") or bronze_sources.get("project")
        if df is None:
            logger.warning("Không tìm thấy bronze DataFrame cho projects")
            return None
        return df

    def transform(self, df):
        """Ánh xạ cột, sinh metadata SCD Type 2 và Surrogate Key."""
        if df is None:
            return None

        # 1. Chuẩn hóa tên trường nghiệp vụ từ pruned contract
        mapped = df.select(
            F.col("sysid").alias("project_id"),
            F.col("name").alias("project_name"),
            F.col("project_type"),
            F.coalesce(F.col("track_status"), F.lit("Unknown")).alias("track_status"),
            F.coalesce(F.col("state"), F.lit("Active")).alias("state"),
            F.coalesce(F.col("percent_completed"), F.lit(0.0)).alias("percent_completed"),
            F.coalesce(F.col("c_department"), F.lit("UNKNOWN")).alias("department_id"),
            F.coalesce(F.col("c_assignee"), F.lit("UNKNOWN")).alias("assignee_id"),
            F.coalesce(F.col("created_by"), F.lit("UNKNOWN")).alias("assignor_id"),
            F.coalesce(F.col("project_manager"), F.col("manager"), F.lit("UNKNOWN")).alias("project_manager_id"),
            F.col("parent").alias("parent_id"),
            F.col("parent_project").alias("parent_project_id"),
            F.col("c_associated_objective").alias("associated_objective_id"),
            F.col("external_id"),
            F.coalesce(F.col("created_on"), F.col("last_updated_on"), F.current_date()).alias("created_on"),
            F.coalesce(F.col("last_updated_on"), F.current_date()).alias("last_updated_on"),
            F.when(F.col("name").startswith("Inferred Stub"), F.lit(True)).otherwise(F.lit(False)).alias("_is_inferred")
        )

        # 2. Xử lý SCD Type 2
        # Cột mốc hiệu lực: valid_from lấy từ created_on hoặc last_updated_on
        base_with_dates = mapped.withColumn(
            "valid_from", F.col("created_on").cast("date")
        ).withColumn(
            "valid_to", F.to_date(F.lit(self.EXPIRY_DATE_STR))
        ).withColumn(
            "is_current", F.lit(True)
        ).withColumn(
            "version", F.lit(1).cast("integer")
        )

        # 3. Gán Surrogate Key (project_sk) theo thứ tự project_id và valid_from
        window_spec = Window.orderBy("project_id", "valid_from")
        final_df = base_with_dates.withColumn(
            "project_sk", F.row_number().over(window_spec).cast("integer")
        ).withColumn(
            "_loaded_at", F.current_date()
        )

        logger.info(json.dumps({
            "event": "dim_project_scd2_transformed",
            "dimension": self.dim_name,
            "monitored_columns": self.SCD2_MONITORED_COLUMNS
        }))
        return final_df

    def merge_scd2(self, incoming_df, existing_df):
        """Hợp nhất SCD Type 2 giữa dữ liệu lịch sử và batch mới (Spark 2.3.2 compatible).
        
        Nếu thuộc tính giám sát (PM, Dept, Status, BSC) thay đổi:
          - Đóng record cũ: valid_to = incoming.valid_from, is_current = False
          - Thêm record mới: valid_from = incoming.last_updated_on, valid_to = 9999-12-31, is_current = True
        Nếu không đổi:
          - Giữ nguyên hoặc update thông tin tiến độ (% completed) trên bản ghi hiện hành.
        """
        if existing_df is None:
            return incoming_df

        # Tách bản ghi lịch sử đã đóng (is_current == False) -> giữ nguyên 100%
        closed_history = existing_df.filter(F.col("is_current") == False)

        # Lấy bản ghi hiện hành từ existing
        current_existing = existing_df.filter(F.col("is_current") == True)

        # Join incoming với current_existing theo project_id
        joined = incoming_df.alias("inc").join(
            current_existing.alias("curr"),
            F.col("inc.project_id") == F.col("curr.project_id"),
            "full_outer"
        )

        # Phát hiện thay đổi trên các trường SCD2_MONITORED_COLUMNS
        has_changed_expr = F.lit(False)
        for col in self.SCD2_MONITORED_COLUMNS:
            has_changed_expr = has_changed_expr | (
                F.coalesce(F.col(f"inc.{col}"), F.lit("")) != F.coalesce(F.col(f"curr.{col}"), F.lit(""))
            )

        # Các bản ghi cần đóng phiên bản cũ
        expiring_records = joined.filter(
            F.col("curr.project_id").isNotNull() & F.col("inc.project_id").isNotNull() & has_changed_expr
        ).select(
            "curr.*"
        ).withColumn(
            "valid_to", F.coalesce(F.col("inc.last_updated_on"), F.current_date())
        ).withColumn(
            "is_current", F.lit(False)
        )

        # Các bản ghi mới mở phiên bản kế tiếp
        new_version_records = joined.filter(
            F.col("curr.project_id").isNotNull() & F.col("inc.project_id").isNotNull() & has_changed_expr
        ).select(
            "inc.*"
        ).withColumn(
            "valid_from", F.coalesce(F.col("inc.last_updated_on"), F.current_date())
        ).withColumn(
            "valid_to", F.to_date(F.lit(self.EXPIRY_DATE_STR))
        ).withColumn(
            "is_current", F.lit(True)
        ).withColumn(
            "version", F.col("curr.version") + 1
        )

        # Các bản ghi không thay đổi hoặc dự án hoàn toàn mới
        unchanged_or_new = joined.filter(
            (~has_changed_expr) | F.col("curr.project_id").isNull()
        ).select(
            F.coalesce(F.col("inc.project_id"), F.col("curr.project_id")),
            "inc.*"
        )

        # Union toàn bộ và tính lại Surrogate Key duy nhất
        unioned = closed_history.union(expiring_records).union(new_version_records).union(unchanged_or_new)
        window_spec = Window.orderBy("project_id", "valid_from")
        return unioned.withColumn("project_sk", F.row_number().over(window_spec).cast("integer"))
