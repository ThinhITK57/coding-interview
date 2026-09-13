import logging
import json
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class BaseDimension(ABC):
    """Lớp cơ sở trừu tượng cho tất cả các Dimension Transformers (Kimball Standard).
    
    Quy chuẩn tương thích:
      - Python 3.7.1
      - Apache Spark 2.3.2
      - Trino Hive Catalog
    
    Vòng đời xử lý chuẩn:
      1. extract(): Đọc dữ liệu từ Bronze Parquet (đã pruned & conformed).
      2. transform(): Ánh xạ trường, ép kiểu, tạo Surrogate Key, xử lý SCD Type 1 hoặc Type 2.
      3. load(): Ghi Parquet vào phân vùng data/warehouse/dimensions/<dim_name>.
      4. build(): Pipeline khép kín extract -> transform -> load.
    """

    def __init__(self, spark_session, dim_name: str, output_base: str = "./data/warehouse/dimensions"):
        self.spark = spark_session
        self.dim_name = dim_name
        self.output_base = output_base.rstrip("/")
        self.output_path = f"{self.output_base}/{self.dim_name}"

    @abstractmethod
    def extract(self, bronze_sources: Dict[str, Any]):
        """Đọc và lọc các DataFrame nguồn từ tầng Bronze."""
        pass

    @abstractmethod
    def transform(self, extracted_data: Any):
        """Thực hiện chuẩn hóa thuộc tính, hierarchy và gán Surrogate Key."""
        pass

    def load(self, df, mode: str = "overwrite", partition_by: Optional[List[str]] = None):
        """Ghi DataFrame ra Parquet tuân thủ cấu hình legacy format cho Spark 2.3.2."""
        writer = df.write.mode(mode).format("parquet")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(self.output_path)
        logger.info(json.dumps({
            "event": "dimension_loaded",
            "dimension": self.dim_name,
            "output_path": self.output_path,
            "mode": mode
        }))

    def build(self, bronze_sources: Dict[str, Any], mode: str = "overwrite"):
        """Thực thi trọn vẹn luồng tạo Dimension."""
        logger.info(json.dumps({
            "event": "dimension_build_start",
            "dimension": self.dim_name
        }))
        extracted = self.extract(bronze_sources)
        transformed = self.transform(extracted)
        self.load(transformed, mode=mode)
        return transformed
