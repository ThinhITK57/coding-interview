from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class VersionDeduplicator:
    """
    Xử lý xung đột đa phiên bản (Multi-version concurrency) trong khoảng thời gian hẹp.
    Chỉ giữ lại bản ghi có timestamp mới nhất và version cao nhất cho mỗi business_id.
    """
    @staticmethod
    def deduplicate(df: DataFrame, business_key: str, timestamp_col: str, version_col: str = None) -> DataFrame:
        if version_col and version_col in df.columns:
            order_exprs = [F.col(timestamp_col).desc(), F.col(version_col).desc()]
        else:
            order_exprs = [F.col(timestamp_col).desc()]

        window_spec = Window.partitionBy(business_key).orderBy(*order_exprs)

        df_dedup = df.withColumn("_row_num", F.row_number().over(window_spec)) \
                     .filter(F.col("_row_num") == 1) \
                     .drop("_row_num")
        return df_dedup
