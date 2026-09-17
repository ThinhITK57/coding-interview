import logging
import json

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
logger = logging.getLogger(__name__)


class StatisticsProfiler:
    def profile(self, df):
        total_records = df.count()
        if total_records == 0: 
            logger.info(json.dumps({
                "event": "profile_empty_dataframe"
            }))
            return {
                "total_records": 0,
                "column": {}
            }
        
        column_stats = {}
        fields = [
            field
            for field in df.schema.fields
            if not field.name.startswith("_")
        ]
        if not fields:
            return {
                "total_records": total_records,
                "columns": {}
            }
        
        agg_expressions = []
        for field in fields:
            col_name = field.name
            col = F.col(col_name)

            agg_expressions.append(
                F.sum(
                    F.when(col.isNull(), 1).otherwise(0)
                ).alias("__null__" + col_name)
            )

            agg_expressions.append(
                F.countDistinct(col).alias(
                    "__distinct__" + col_name
                )
            )

            # Min
            agg_expressions.append(
                F.min(col).alias(
                    "__min__" + col_name
                )
            )

            # Max
            agg_expressions.append(
                F.max(col).alias(
                    "__max__" + col_name
                )
            )

            if isinstance(field.dataType, NumericType):
                agg_expressions.append(
                    F.mean(col).alias(
                        "__mean__" + col_name
                    )
                )
        logger.info(
            "StatisticsProfiler: executing aggregation for %d columns",
            len(fields)
        )

        stats_row = df.agg(*agg_expressions).collect()[0]
        logger.info(
            "StatisticsProfiler : Aggregation completed"
        )
        for field in fields:
            col_name = field.name
            null_count = stats_row[
                "__null__" + col_name
            ]
            distinct_count = stats_row[
                "__distinct__" + col_name
            ]

            min_value = stats_row[
                "__min__" + col_name
            ]

            max_value = stats_row[
                "__max__" + col_name
            ]

            null_ratio = (
                float(null_count) / total_records
                if total_records > 0
                else 0.0
            )

            col_stats = {
                "null_count": int(null_count or 0),
                "null_ratio": round(null_ratio, 6),
                "distinct_count": int(distinct_count or 0),
                "min": None,
                "max": None,
                "mean": None
            }

            if isinstance(field.dataType, NumericType):
                col_stats["min"] = min_value
                col_stats["max"] = max_value
                mean_value = stats_row[
                    "__mean__" + col_name
                ]

                if mean_value is not None:
                    col_stats["mean"] = round(float(mean_value), 4)
            else:
                if min_value is not None:
                    col_stats["min"] = str(min_value)

                if max_value is not None:
                    col_stats["max"] = str(max_value)

            column_stats[col_name] = col_stats

        result = {
            "total_records": total_records,
            "columns": column_stats
        }

        logger.info(json.dumps({
            "event": "profile_complete",
            "total_records": total_records,
            "columns_profiled": len(column_stats)
        }))

        #     stats_row = df.agg(
        #         F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count"),
        #         F.countDistinct(F.col(col_name)).alias("distinct_count")
        #     ).collect()[0]

        #     null_count = stats_row["null_count"]
        #     distinct_count = stats_row["distinct_count"]

        #     null_ratio = null_count / total_records if total_records > 0 else 0.0

        #     col_stats = {
        #         "null_count": null_count,
        #         "null_ratio": round(null_ratio, 6),
        #         "distinct_count": distinct_count,
        #         "min": None, 
        #         "max": None,
        #         "mean": None
        #     }

        #     # Compute min, max, mean for numeric column
        #     is_numeric = isinstance(field.dataType, NumericType)
        #     if is_numeric:
        #         agg_row = df.agg(
        #             F.min(F.col(col_name)).alias("min_val"),
        #             F.max(F.col(col_name)).alias("max_val"),
        #             F.mean(F.col(col_name)).alias("mean_val")
        #         ).collect()[0]
        #         col_stats["min"] = agg_row["min_val"]
        #         col_stats["max"] = agg_row["max_val"]
        #         col_stats["mean"] = (round(float(agg_row["mean_val"]), 4) if agg_row["mean_val"] is not None else None)

        #     else:
        #         agg_row = df.agg(
        #             F.min(F.col(col_name)).alias("min_val"),
        #             F.max(F.col(col_name)).alias("max_val"),
        #         ).collect()[0]
        #         col_stats["min"] = str(agg_row["min_val"]) if agg_row["min_val"] is not None else None
        #         col_stats["max"] = str(agg_row["max_val"]) if agg_row["max_val"] is not None else None

        #     column_stats[col_name] = col_stats

        # result = {
        #     "total_records": total_records,
        #     "columns": column_stats
        # }

        # logger.info(json.dumps({
        #     "event": "profile_complete",
        #     "total_records": total_records,
        #     "columns_profiled": len(column_stats)
        # }))

        return result

