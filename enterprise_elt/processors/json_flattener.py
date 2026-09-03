from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import functions as F

class SparkJSONFlattener:
    """
    Xử lý đệ quy làm phẳng schema JSON lồng nhau cho Spark 2.3.2.
    Biến đổi mọi trường con dạng 'a.b.c' thành 'a_b_c'.
    """
    @classmethod
    def flatten(cls, df: DataFrame) -> DataFrame:
        flat_cols = []
        has_nested = False

        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                has_nested = True
                for sub_field in field.dataType.fields:
                    flat_cols.append(
                        F.col(f"`{field.name}`.`{sub_field.name}`").alias(f"{field.name}_{sub_field.name}")
                    )
            elif isinstance(field.dataType, ArrayType):
                # Giữ nguyên cấu trúc mảng hoặc convert sang JSON string để bảo toàn nguyên vẹn
                flat_cols.append(F.col(f"`{field.name}`"))
            else:
                flat_cols.append(F.col(f"`{field.name}`"))

        df_result = df.select(*flat_cols)

        if has_nested:
            return cls.flatten(df_result)
        return df_result
