import logging
import json
from typing import Dict, Any, List

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimDepartmentBuilder(BaseDimension):
    """Xây dựng bảng dim_department chuẩn Kimball thông qua Cross-Table Union.
    
    Department trong Clarizen xuất hiện dưới dạng chuỗi hoặc URI định danh
    (ví dụ: '/C_Department/IT_Department') nằm rải rác ở 5 bảng:
    project, task, bsc (objective), c_assignment, target.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_department", output_base=output_base)

    def extract(self, bronze_sources: Dict[str, Any]) -> List[Any]:
        """Trích xuất distinct c_department từ tất cả các bảng bronze có trường này."""
        dept_dfs = []
        dept_col_candidates = ["c_department", "department"]

        for source_name, df in bronze_sources.items():
            if df is None:
                continue
            matched_col = next((c for c in dept_col_candidates if c in df.columns), None)
            if matched_col:
                selected = (
                    df.select(F.col(matched_col).alias("raw_dept"))
                    .filter(F.col("raw_dept").isNotNull() & (F.trim(F.col("raw_dept")) != ""))
                    .distinct()
                )
                dept_dfs.append(selected)

        return dept_dfs

    def transform(self, extracted_dfs: List[Any]):
        """Union, chuẩn hóa tên phòng ban, và gán Surrogate Key (SK) tuần tự."""
        if not extracted_dfs:
            # Tạo DataFrame rỗng nếu chưa có nguồn
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
            schema = StructType([
                StructField("department_sk", IntegerType(), False),
                StructField("department_id", StringType(), False),
                StructField("department_name", StringType(), False),
                StructField("_loaded_at", DateType(), False)
            ])
            return self.spark.createDataFrame([], schema)

        combined = extracted_dfs[0]
        for extra in extracted_dfs[1:]:
            combined = combined.union(extra)

        distinct_depts = combined.distinct()

        # Bóc tách tên hiển thị: nếu dạng URI '/C_Department/IT' -> lấy 'IT'
        parsed = distinct_depts.withColumn(
            "clean_id", F.trim(F.col("raw_dept"))
        ).withColumn(
            "clean_name",
            F.when(
                F.col("clean_id").contains("/"),
                F.regexp_extract(F.col("clean_id"), r"/([^/]+)$", 1)
            ).otherwise(F.col("clean_id"))
        )

        # Gán Surrogate Key (1..N) bằng Window Function tương thích Spark 2.3.2
        window_spec = Window.orderBy("clean_id")
        ranked = parsed.withColumn("department_sk", F.row_number().over(window_spec))

        dim_df = ranked.select(
            F.col("department_sk").cast("integer"),
            F.col("clean_id").alias("department_id"),
            F.col("clean_name").alias("department_name"),
            F.current_date().alias("_loaded_at")
        )

        # Thêm bản ghi mặc định cho trường hợp NULL / UNKNOWN (department_sk = 0)
        unknown_record = self.spark.createDataFrame([
            (0, "UNKNOWN", "Chưa xác định", None)
        ], ["department_sk", "department_id", "department_name", "_loaded_at"]).withColumn(
            "_loaded_at", F.current_date()
        )

        final_df = unknown_record.union(dim_df)
        logger.info(json.dumps({
            "event": "dim_department_transformed",
            "dimension": self.dim_name
        }))
        return final_df
