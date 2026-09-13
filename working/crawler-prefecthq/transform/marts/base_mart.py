import logging
import json
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class BaseMart(ABC):
    """Lớp cơ sở trừu tượng cho 5 Semantic Mart Builders (Materialized Views).
    
    Quy chuẩn thiết kế (Theo triết lý triệt tiêu Cold-start cho người dùng):
      - Đọc đồng thời các bảng Dimension và Fact.
      - Thực hiện JOINs, Window Functions (Ranking, Percentile, Lag/Lead, Partition Averages).
      - Tính toán sẵn >10 chỉ số phân tích chuyên sâu cho mỗi Mart.
      - Materialize ra Parquet tại data/warehouse/marts/<mart_name> phục vụ truy vấn tức thì.
    """

    def __init__(self, spark_session, mart_name: str, output_base: str = "./data/warehouse/marts"):
        self.spark = spark_session
        self.mart_name = mart_name
        self.output_base = output_base.rstrip("/")
        self.output_path = f"{self.output_base}/{self.mart_name}"

    @abstractmethod
    def extract(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any]) -> Any:
        """Nạp các bảng Dimension và Fact liên quan đến Mart này."""
        pass

    @abstractmethod
    def build_mart(self, extracted_data: Any) -> Any:
        """Thực thi logic tính toán các chỉ số Pre-computed Intelligence."""
        pass

    def load(self, df, mode: str = "overwrite", partition_by: Optional[List[str]] = None):
        """Materialize Mart ra Parquet với Snappy compression tương thích Spark 2.3.2."""
        writer = df.write.mode(mode).format("parquet")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(self.output_path)
        logger.info(json.dumps({
            "event": "semantic_mart_loaded",
            "mart": self.mart_name,
            "output_path": self.output_path,
            "mode": mode
        }))

    def build(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any], mode: str = "overwrite"):
        """Quy trình khép kín: Nạp Dims & Facts -> Pre-compute Mart -> Materialize."""
        logger.info(json.dumps({
            "event": "semantic_mart_build_start",
            "mart": self.mart_name
        }))
        extracted = self.extract(dim_tables, fact_tables)
        mart_df = self.build_mart(extracted)
        self.load(mart_df, mode=mode)
        return mart_df
