import os
import json

import logging
from datetime import datetime
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


class DLQRouter:
    def __init__(
            self,
            base_storage_dir: str = "./data",
            dlq_schema: str = "dlq",
            trino_catalog: str = "hive"
        ):
        self.base_storage_dir = base_storage_dir.rstrip("/")
        self.dlq_schema = dlq_schema
        self.trino_catalog = trino_catalog

    def route_dlq(
            self,
            dlq_df,
            table_name: str,
            batch_id: str,
            partition_col: str = "ingest_date",
            partition_val: Optional[str] = None
    )-> Dict[str, Any]:
        if dlq_df is None or dlq_df.rdd.isEmpty():
            return {"dlq_count": 0, "status": "NO_ERRORS"}

        import pyspark.sql.functions as F

        today_str = partition_val or datetime.utcnow().strftime("%Y-%m-%d")
        dlq_target_dir = os.path.join(
            self.base_storage_dir,
            "warehouse",
            self.dlq_schema,
            table_name.lower()
        )

        # Enrich with DLQ metadata
        enriched_dlq = (
            dlq_df
            .withColumn("_dlq_routed_at",F.current_timestamp())
            .withColumn("_dlq_batch_id", F.lit(batch_id))
            .withColumn("_dlq_source_table", F.lit(table_name))
        )

        if partition_col not in enriched_dlq.columns:
            enriched_dlq = enriched_dlq.withColumn(partition_col, F.to_date(F.lit(today_str)))


        dlq_count = enriched_dlq.count()
        if dlq_count == 0:
            return {
                "table_name": table_name,
                "dlq_count": 0,
                "dlq_path": dlq_target_dir,
                "error_breakdown": {},
                "batch_id": batch_id,
                "status": "NO_ERRORS"
            }

        # Write partitioned Parquet for DLQ
        (
            enriched_dlq.write.mode("append").format("parquet").option("compression","snappy").partitionBy(partition_col)
            .save(dlq_target_dir)
        )

        # Extract error frequency breakdown
        error_breakdown = {}
        if "_error_tags" in enriched_dlq.columns:
            tag_counts = (
                enriched_dlq
                .select(
                    F.explode(
                        F.split(F.col("_error_tags"), ";")
                    )
                )
                .withColumnRenamed("col", "error_tag")
                .filter(
                    F.length(F.trim(F.col("error_tag"))) > 0
                )
                .groupBy("error_tag")
                .count()
            )

            for row in tag_counts.toLocalIterator():
                error_breakdown[row["error_tag"]] = row["count"]

        summary = {
            "table_name": table_name,
            "dlq_count": dlq_count,
            "dlq_path": dlq_target_dir,
            "error_breakdown": error_breakdown,
            "batch_id": batch_id
        }

        logger.warning(json.dumps({
            "event": "dlq_records_quarantined",
            **summary
        }))

        return summary