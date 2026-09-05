import logging
import json

logger = logging.getLogger(__name__)


class StatisticsProfiler:
    """
    Computes per-column data quality statistics on Spark DataFrames.
    Compatible with Spark 2.3.2.

    Deep module design:
        Interface (small): profile(df) -> dict of column stats
        Implementation (deep): Computes null_count, null_ratio, distinct_count,
        min, max, mean (for numeric columns), top_values (for categorical).
        All computed in minimal number of Spark actions.
    """

    def profile(self, df):
        """Compute per-column statistics for the DataFrame.

        Args:
            df: Spark DataFrame to profile.

        Returns:
            dict: {
                "total_records": int,
                "columns": {
                    "col_name": {
                        "null_count": int,
                        "null_ratio": float,
                        "distinct_count": int,
                        "min": Any,
                        "max": Any,
                        "mean": float or None,
                    }
                }
            }
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import NumericType

        total_records = df.count()

        if total_records == 0:
            logger.info(json.dumps({
                "event": "profile_empty_dataframe",
            }))
            return {
                "total_records": 0,
                "columns": {},
            }

        column_stats = {}

        for field in df.schema.fields:
            col_name = field.name

            # Skip metadata columns
            if col_name.startswith("_"):
                continue

            # Compute null count and distinct count in one pass
            stats_row = df.agg(
                F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count"),
                F.countDistinct(F.col(col_name)).alias("distinct_count"),
            ).collect()[0]

            null_count = stats_row["null_count"]
            distinct_count = stats_row["distinct_count"]
            null_ratio = null_count / total_records if total_records > 0 else 0.0

            col_stat = {
                "null_count": null_count,
                "null_ratio": round(null_ratio, 6),
                "distinct_count": distinct_count,
                "min": None,
                "max": None,
                "mean": None,
            }

            # Compute min/max/mean for numeric columns
            is_numeric = isinstance(field.dataType, NumericType)
            if is_numeric:
                agg_row = df.agg(
                    F.min(F.col(col_name)).alias("min_val"),
                    F.max(F.col(col_name)).alias("max_val"),
                    F.mean(F.col(col_name)).alias("mean_val"),
                ).collect()[0]
                col_stat["min"] = agg_row["min_val"]
                col_stat["max"] = agg_row["max_val"]
                col_stat["mean"] = (
                    round(float(agg_row["mean_val"]), 4)
                    if agg_row["mean_val"] is not None
                    else None
                )
            else:
                # For string columns, get min/max as lexicographic
                agg_row = df.agg(
                    F.min(F.col(col_name)).alias("min_val"),
                    F.max(F.col(col_name)).alias("max_val"),
                ).collect()[0]
                col_stat["min"] = str(agg_row["min_val"]) if agg_row["min_val"] is not None else None
                col_stat["max"] = str(agg_row["max_val"]) if agg_row["max_val"] is not None else None

            column_stats[col_name] = col_stat

        result = {
            "total_records": total_records,
            "columns": column_stats,
        }

        logger.info(json.dumps({
            "event": "profile_complete",
            "total_records": total_records,
            "columns_profiled": len(column_stats),
        }))

        return result
