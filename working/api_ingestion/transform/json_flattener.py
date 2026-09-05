import logging
import json

logger = logging.getLogger(__name__)


class JSONFlattener:
    """
    Recursively flatten nested JSON structures into flat Spark DataFrames.
    Compatible with Spark 2.3.2.

    Deep module design:
        Interface (small): flatten(df) -> flattened_df
        Implementation (deep): Recursive StructType field detection and expansion,
        ArrayType explode handling, column renaming with parent_child convention,
        tracking of flatten paths for docstring generation.

    Example:
        Input:  {"ParentProject": {"Name": "Proj A", "Id": "123"}}
        Output: {"parent_project_name": "Proj A", "parent_project_id": "123"}
    """

    def __init__(self, separator="_", max_depth=10):
        """Initialize flattener.

        Args:
            separator: Separator for nested column names (default: "_").
            max_depth: Maximum recursion depth to prevent infinite loops.
        """
        self._separator = separator
        self._max_depth = max_depth
        self._flatten_map = {}  # Maps flattened_col -> original_path

    def flatten(self, df):
        """Recursively flatten all nested StructType and ArrayType fields.

        Args:
            df: Input Spark DataFrame with potentially nested columns.

        Returns:
            Spark DataFrame with all fields flattened to top-level columns.
        """
        from pyspark.sql.types import StructType, ArrayType

        self._flatten_map = {}
        result_df = self._flatten_recursive(df, depth=0)

        logger.info(json.dumps({
            "event": "json_flatten_complete",
            "input_columns": len(df.columns),
            "output_columns": len(result_df.columns),
            "flatten_paths": len(self._flatten_map),
        }))

        return result_df

    def _flatten_recursive(self, df, prefix="", depth=0):
        """Recursively expand StructType fields and explode ArrayType fields."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, ArrayType

        if depth >= self._max_depth:
            logger.warning(json.dumps({
                "event": "flatten_max_depth_reached",
                "prefix": prefix,
                "depth": depth,
            }))
            return df

        has_nested = False
        select_cols = []

        for field in df.schema.fields:
            col_name = field.name
            full_name = f"{prefix}{self._separator}{col_name}" if prefix else col_name

            if isinstance(field.dataType, StructType):
                has_nested = True
                # Expand struct fields with parent prefix
                for sub_field in field.dataType.fields:
                    sub_col_name = self._to_snake_case(
                        f"{col_name}{self._separator}{sub_field.name}"
                    )
                    select_cols.append(
                        F.col(f"`{col_name}`.`{sub_field.name}`").alias(sub_col_name)
                    )
                    # Track flatten path for docstring generation
                    original_path = f"{prefix}.{col_name}.{sub_field.name}" if prefix else f"{col_name}.{sub_field.name}"
                    self._flatten_map[sub_col_name] = original_path

            elif isinstance(field.dataType, ArrayType):
                has_nested = True
                exploded_name = self._to_snake_case(f"{col_name}_item")
                # Explode array and rename
                df = df.withColumn(exploded_name, F.explode_outer(F.col(f"`{col_name}`")))
                select_cols.append(F.col(f"`{exploded_name}`"))
                self._flatten_map[exploded_name] = f"{col_name}[*]"
                # Remove original array column from selection

            else:
                snake_name = self._to_snake_case(col_name)
                if snake_name != col_name:
                    select_cols.append(F.col(f"`{col_name}`").alias(snake_name))
                    self._flatten_map[snake_name] = col_name
                else:
                    select_cols.append(F.col(f"`{col_name}`"))

        if not has_nested:
            return df

        # Select expanded columns
        result_df = df.select(select_cols)

        # Recurse to handle nested structs within structs
        return self._flatten_recursive(result_df, prefix=prefix, depth=depth + 1)

    def _to_snake_case(self, name):
        """Convert CamelCase or mixed names to snake_case.

        Examples:
            ParentProject_Name -> parent_project_name
            SYSID -> sysid
            PercentCompleted -> percent_completed
        """
        import re
        # Insert underscore before uppercase letters preceded by lowercase
        s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        # Insert underscore before uppercase letters followed by lowercase (for sequences like "SYSID")
        s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s)
        # Replace non-alphanumeric with underscore
        s = re.sub(r'[^a-zA-Z0-9]', '_', s)
        # Collapse multiple underscores
        s = re.sub(r'_+', '_', s)
        return s.lower().strip('_')

    @property
    def flatten_map(self):
        """Return mapping of flattened column names to original JSON paths.

        Returns:
            dict: {"parent_project_name": "ParentProject.Name", ...}
        """
        return dict(self._flatten_map)
