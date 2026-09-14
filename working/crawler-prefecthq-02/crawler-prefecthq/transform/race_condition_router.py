import logging
import json
from typing import Dict, List, Optional

from datetime import datetime

logger = logging.getLogger(__name__)


class InferredDimensionRouter:
    def __init__(self, epoch_timestamp: str = "1970-01-01T00:00:00Z"):
        self.epoch_timestamp = epoch_timestamp

    def generate_inferred_stubs(
            self,
            fact_df,
            fk_column: str,
            dim_pk_column: str = "sysid",
            dim_name_column: str = "name",
            partition_col: str = "ingest_date",
            partition_val: Optional[str] = None
    ):
        import pyspark.sql.functions as F
        if fk_column not in fact_df.columns:
            logger.info("FK column '%s' not present in Fact dataframe; skipping inferred dims.", fk_column)
            return None

        today_str = partition_val or datetime.utcnow().strftime("%Y-%m-%d")

        distinct_fks = (
            fact_df
            .select(F.col(fk_column).alias(dim_pk_column))
            .filter(F.col(dim_pk_column).isNotNull() & (F.length(F.trim(F.col(dim_pk_column))) > 0))
            .distinct()
        )

        if distinct_fks.rdd.isEmpty():
            return None

        stubs_df = (
            distinct_fks
            .withColumn(dim_name_column, F.concat(F.lit("Inferred Stub ["), F.col(dim_pk_column), F.lit("]")))
            .withColumn("last_modified", F.lit(self.epoch_timestamp))
            .withColumn("_is_inferred", F.lit(True))
            .withColumn("_inferred_timestamp", F.current_timestamp())
            .withColumn("_batch_id", F.lit("INFERRED_STUB_AUTO_GEN"))
            .withColumn(partition_col, F.to_date(F.lit(today_str)))
        )

        stub_count = stubs_df.count()

        logger.info(json.dumps({
            "event": "inferred_dimensions_stubs_generated",
            "fk_columns": fk_column,
            "stub_count": stub_count
        }))

        return stubs_df

    def reconcile_dimension(
            self,
            stubs_df, 
            dim_table_path: str,
            spark_session,
            dedup_engine,
            dim_pk_column: str = "sysid",
            partition_col: str = "ingest_date"
    ):
        """Return number of stubs inserted or reconciled"""
        if stubs_df is None or stubs_df.rdd.isEmpty():
            return 0


        merged_dim_df = dedup_engine.merge_with_existing(
            incoming_df=stubs_df,
            existing_storage_path=dim_table_path,
            spark_session=spark_session,
            primary_key=dim_pk_column,
            watermark_col="last_modified",
            partition_col=partition_col
        )

        (
            merged_dim_df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .partitionBy(partition_col)
            .save(dim_table_path)
        )

        logger.info(json.dumps({
            "event": "inferred_dimension_reconciled_to_storage",
            "dim_path": dim_table_path,
            "total_dim_records": merged_dim_df.count()
        }))

        return stubs_df.count()