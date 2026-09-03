from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataQualityEngine:
    """
    Rule Engine kiểm soát chất lượng dữ liệu:
    - Single-pass tagging: Gắn mã lỗi vào cột 'validation_errors'.
    - Hoàn toàn tương thích Spark 2.3.2 (dùng concat_ws thay vì array_compact).
    """
    def __init__(self, rules_config: list):
        self.rules_config = rules_config

    def validate_and_split(self, df: DataFrame):
        error_tag_expressions = []
        for r in self.rules_config:
            # Nếu vi phạm condition -> lấy mã lỗi r['code'], ngược lại lấy chuỗi rỗng
            error_tag_expressions.append(
                F.when(r["cond"], F.lit(r["code"])).otherwise(F.lit(""))
            )

        # Spark 2.3.2: Nối các chuỗi lỗi lại, phân cách bởi dấu ';'
        df_tagged = df.withColumn(
            "validation_errors",
            F.concat_ws(";", *error_tag_expressions)
        )

        # Đánh dấu dữ liệu hợp lệ và dữ liệu DLQ
        # Spark 2.3: dùng F.length == 0 thay vì size(array) == 0
        df_valid = df_tagged.filter(F.length(F.col("validation_errors")) == 0).drop("validation_errors")
        df_dlq   = df_tagged.filter(F.length(F.col("validation_errors")) > 0)

        return df_valid, df_dlq
