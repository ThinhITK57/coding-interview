import logging
import json
from datetime import date, timedelta
from typing import Dict, Any, Optional

from transform.dimensions.base_dimension import BaseDimension

logger = logging.getLogger(__name__)


class DimDateGenerator(BaseDimension):
    """Sinh bảng dim_date (Date Spine) từ năm 2020 đến 2030.
    
    Bảng này được sinh độc lập mà không cần API nguồn, cung cấp trục thời gian
    chuẩn hóa cho toàn bộ Fact tables và Time-travel query của GenBI.
    """

    MONTH_NAMES = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    DAY_NAMES = [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    ]

    def __init__(self, spark_session, start_year: int = 2020, end_year: int = 2030, output_base: str = "./data/warehouse/dimensions"):
        super().__init__(spark_session, dim_name="dim_date", output_base=output_base)
        self.start_date = date(start_year, 1, 1)
        self.end_date = date(end_year, 12, 31)

    def extract(self, bronze_sources: Optional[Dict[str, Any]] = None):
        """Date spine không phụ thuộc nguồn bronze."""
        return None

    def transform(self, extracted_data: Any = None):
        """Sinh dữ liệu ngày đầy đủ thuộc tính trên bộ nhớ và chuyển thành Spark DataFrame."""
        from pyspark.sql.types import (
            StructType, StructField, IntegerType, DateType, StringType, BooleanType
        )

        schema = StructType([
            StructField("date_key", IntegerType(), False),
            StructField("full_date", DateType(), False),
            StructField("year", IntegerType(), False),
            StructField("quarter", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("month_name", StringType(), False),
            StructField("week_of_year", IntegerType(), False),
            StructField("day_of_week", IntegerType(), False),
            StructField("day_name", StringType(), False),
            StructField("is_weekend", BooleanType(), False),
            StructField("fiscal_year", IntegerType(), False),
            StructField("fiscal_quarter", IntegerType(), False),
        ])

        rows = []
        curr = self.start_date
        delta = timedelta(days=1)

        while curr <= self.end_date:
            date_key = curr.year * 10000 + curr.month * 100 + curr.day
            quarter = (curr.month - 1) // 3 + 1
            day_of_week = curr.isoweekday()  # 1: Mon, 7: Sun
            is_weekend = day_of_week in (6, 7)
            week_of_year = curr.isocalendar()[1]

            # Năm tài chính chuẩn (đồng nhất năm dương lịch, có thể tùy chỉnh)
            fiscal_year = curr.year
            fiscal_quarter = quarter

            rows.append((
                date_key,
                curr,
                curr.year,
                quarter,
                curr.month,
                self.MONTH_NAMES[curr.month],
                week_of_year,
                day_of_week,
                self.DAY_NAMES[day_of_week - 1],
                is_weekend,
                fiscal_year,
                fiscal_quarter,
            ))
            curr += delta

        df = self.spark.createDataFrame(rows, schema)
        logger.info(json.dumps({
            "event": "dim_date_generated",
            "total_days": len(rows),
            "start_date": str(self.start_date),
            "end_date": str(self.end_date)
        }))
        return df
