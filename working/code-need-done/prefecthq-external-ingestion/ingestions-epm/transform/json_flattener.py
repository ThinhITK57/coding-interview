import logging
import json

logger = logging.getLogger(__name__)


class JSONFlattener:
    def __init__(self, separator="_", max_depth=10):
        self._separator = separator
        self._max_depth = max_depth
        self._flatten_map = {}

    def flatten(self, df):
        from pyspark.sql.types import StructType, ArrayType
        self._flatten_map = {}

        result_df = self._flatten_recursive(df, depth=0)

        logger.info(json.dumps({
            "event": "json_flatten_complete",
            "input_columns": len(df.columns),
            "output_columns": len(result_df.columns),
            "flatten_paths": len(self._flatten_map)
        }))

        return result_df

    def _flatten_recursive(self, df, prefix="", depth=0):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, ArrayType

        if depth >= self._max_depth:
            logger.warning(json.dumps({
                "event": "flatten_max_depth_reached",
                "prefix": prefix,
                "depth": depth
            }))
            return df

        has_nested = False
        select_cols = []

        for field in df.schema.fields:
            col_name = field.name
            full_name = f"{prefix}{self._separator}{col_name}" if prefix else col_name
            if isinstance(field.dataType, StructType):
                has_nested = True

                for sub_field in field.dataType.fields:
                    sub_col_name = self._to_snake_case(
                        f"{col_name}{self._separator}{sub_field.name}"
                    )
                    select_cols.append(
                        F.col(f"`{col_name}`.`{sub_field.name}`").alias(sub_col_name)
                    )
                    original_path = f"{prefix}.{col_name}.{sub_field.name}" if prefix else f"{col_name}.{sub_field.name}"
                    self._flatten_map[sub_col_name] = original_path

            elif isinstance(field.dataType, ArrayType):
                has_nested = True
                exploded_name = self._to_snake_case(f"{col_name}_item")
                df = df.withColumn(exploded_name, F.explode_outer(F.col(f"`{col_name}`")))
                select_cols.append(F.col(f"`{exploded_name}`"))
                self._flatten_map[exploded_name] = f"{col_name}[*]"

            else:
                snake_name = self._to_snake_case(col_name)
                if snake_name != col_name:
                    select_cols.append(F.col(f"`{col_name}`").alias(snake_name))
                    self._flatten_map[snake_name] = col_name
                else:
                    select_cols.append(F.col(f"`{col_name}`"))

        if not has_nested:
            return df

        result_df = df.select(select_cols)
        return self._flatten_recursive(result_df, prefix=prefix, depth=depth + 1)

    def _to_snake_case(self, name):
        import re
        s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s)
        s = re.sub(r'[^a-zA-Z0-9]', '_', s)
        s = re.sub(r'_+', '_', s)
        return s.lower().strip('_')

    @property
    def flatten_map(self):
        return dict(self._flatten_map)
    
        

        