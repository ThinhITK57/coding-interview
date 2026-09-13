import logging
import json
from typing import Dict, Any, List

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimResourceBuilder(BaseDimension):
    """Xây dựng bảng dim_resource (Người dùng / Tài nguyên / Phụ trách).
    
    Trong Clarizen, nhân sự xuất hiện dưới nhiều vai trò và cột khác nhau:
    - c_assignee (Người nhận việc)
    - created_by / c_assignor (Người giao việc / Người tạo)
    - project_manager / manager (Quản lý dự án)
    - entity_owner (Chủ sở hữu thực thể)
    - c_action_resources / c_teams (Nhóm hoặc nguồn lực phối hợp)
    """

    RESOURCE_COLUMNS = [
        "c_assignee", "created_by", "project_manager", "manager",
        "entity_owner", "c_assignor", "c_reporter", "c_action_resources"
    ]

    def __init__(self, spark_session, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_resource", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any]) -> List[Any]:
        """Quét và trích xuất distinct user/resource URIs từ tất cả bảng bronze."""
        extracted_dfs = []

        for source_name, df in bronze_sources.items():
            if df is None:
                continue
            for col_name in self.RESOURCE_COLUMNS:
                if col_name in df.columns:
                    selected = (
                        df.select(F.col(col_name).alias("raw_resource"))
                        .filter(F.col("raw_resource").isNotNull() & (F.trim(F.col("raw_resource")) != ""))
                        .distinct()
                    )
                    extracted_dfs.append(selected)

        return extracted_dfs

    def transform(self, extracted_dfs: List[Any]):
        """Union, bóc tách URI, phân loại resource_type, và gán Surrogate Key."""
        if not extracted_dfs:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
            schema = StructType([
                StructField("resource_sk", IntegerType(), False),
                StructField("resource_id", StringType(), False),
                StructField("resource_name", StringType(), False),
                StructField("resource_type", StringType(), False),
                StructField("_loaded_at", DateType(), False)
            ])
            return self.spark.createDataFrame([], schema)

        combined = extracted_dfs[0]
        for extra in extracted_dfs[1:]:
            combined = combined.union(extra)

        distinct_resources = combined.distinct()

        # Bóc tách tên hiển thị và loại tài nguyên từ URI (ví dụ: '/User/user_123' -> 'user_123')
        parsed = distinct_resources.withColumn(
            "clean_id", F.trim(F.col("raw_resource"))
        ).withColumn(
            "clean_name",
            F.when(
                F.col("clean_id").contains("/"),
                F.regexp_extract(F.col("clean_id"), r"/([^/]+)$", 1)
            ).otherwise(F.col("clean_id"))
        ).withColumn(
            "resource_type",
            F.when(F.col("clean_id").contains("/Placeholder"), F.lit("Placeholder"))
             .when(F.col("clean_id").contains("/Group") | F.col("clean_id").contains("/Team"), F.lit("Team"))
             .when(F.col("clean_id").contains("/User"), F.lit("User"))
             .otherwise(F.lit("Individual"))
        )

        window_spec = Window.orderBy("clean_id")
        ranked = parsed.withColumn("resource_sk", F.row_number().over(window_spec))

        dim_df = ranked.select(
            F.col("resource_sk").cast("integer"),
            F.col("clean_id").alias("resource_id"),
            F.col("clean_name").alias("resource_name"),
            F.col("resource_type"),
            F.current_date().alias("_loaded_at")
        )

        # Record UNKNOWN mặc định cho foreign key bị NULL
        unknown_record = self.spark.createDataFrame([
            (0, "UNKNOWN", "Chưa phân công", "System", None)
        ], ["resource_sk", "resource_id", "resource_name", "resource_type", "_loaded_at"]).withColumn(
            "_loaded_at", F.current_date()
        )

        final_df = unknown_record.union(dim_df)
        logger.info(json.dumps({
            "event": "dim_resource_transformed",
            "dimension": self.dim_name
        }))
        return final_df
