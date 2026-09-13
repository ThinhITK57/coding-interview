import logging
import json
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class BaseFact(ABC):
    """Lớp cơ sở trừu tượng cho tất cả các Fact Table Transformers (Kimball Standard).
    
    Quy chuẩn tương thích:
      - Python 3.7.1
      - Apache Spark 2.3.2
      - Trino Hive Catalog
    
    Đặc thù tầng Fact:
      - Grain rõ ràng (ví dụ: 1 row per task/project per snapshot date).
      - Chứa Foreign Keys liên kết tới các Dimension qua Surrogate Keys hoặc Natural Keys.
      - Chứa Measures (số đo) và Derived Measures (chỉ số tính toán sẵn).
      - Hỗ trợ hàm chuyển đổi ngày chuẩn `add_date_key()` sang số nguyên YYYYMMDD.
    """

    DEFAULT_DATE_KEY_FALLBACK = 19700101

    def __init__(self, spark_session, fact_name: str, output_base: str = "./data/warehouse/facts"):
        self.spark = spark_session
        self.fact_name = fact_name
        self.output_base = output_base.rstrip("/")
        self.output_path = f"{self.output_base}/{self.fact_name}"

    @abstractmethod
    def extract(self, bronze_sources: Dict[str, Any], dim_sources: Optional[Dict[str, Any]] = None):
        """Trích xuất dữ liệu từ các bảng Bronze và Dimensions cần thiết."""
        pass

    @abstractmethod
    def compute_measures(self, extracted_data: Any) -> Any:
        """Thực hiện tính toán measures, derived metrics và gán date keys."""
        pass

    def add_date_key(self, df, source_date_col: str, target_key_col: str):
        """Chuyển đổi cột DATE/TIMESTAMP thành Integer Date Key định dạng YYYYMMDD.
        
        Tương thích tuyệt đối với PySpark 2.3.2 và khớp với dim_date.date_key.
        Nếu giá trị NULL, gán fallback về 19700101.
        """
        if source_date_col not in df.columns:
            return df.withColumn(target_key_col, F.lit(self.DEFAULT_DATE_KEY_FALLBACK).cast("integer"))

        return df.withColumn(
            target_key_col,
            F.when(
                F.col(source_date_col).isNotNull(),
                F.date_format(F.col(source_date_col), "yyyyMMdd").cast("integer")
            ).otherwise(F.lit(self.DEFAULT_DATE_KEY_FALLBACK).cast("integer"))
        )

    def load(self, df, mode: str = "append", partition_by: Optional[List[str]] = None):
        """Ghi Fact DataFrame ra Parquet tuân thủ cấu hình legacy format cho Spark 2.3.2."""
        writer = df.write.mode(mode).format("parquet")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(self.output_path)
        logger.info(json.dumps({
            "event": "fact_table_loaded",
            "fact": self.fact_name,
            "output_path": self.output_path,
            "mode": mode
        }))

    def build(self, bronze_sources: Dict[str, Any], dim_sources: Optional[Dict[str, Any]] = None, mode: str = "append"):
        """Thực thi toàn bộ luồng tạo bảng Fact."""
        logger.info(json.dumps({
            "event": "fact_build_start",
            "fact": self.fact_name
        }))
        extracted = self.extract(bronze_sources, dim_sources=dim_sources)
        transformed = self.compute_measures(extracted)
        self.load(transformed, mode=mode)
        return transformed
