import logging
import json
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class InferredDimensionRouter:
    """
    Solves Fact-Dimension Race Conditions using the Inferred Dimension Pattern (Kimball methodology).

    Problem:
        A Fact record (e.g. Task) arrives referencing a Foreign Key (e.g. Project 'PRJ-9999'),
        but the Dimension table has not ingested 'PRJ-9999' yet.
        Without this router: The Fact record either drops, sends to DLQ, or breaks BI joins.

    Solution:
        1. Identifies foreign keys referenced in incoming Fact records.
        2. Generates 'Inferred Stub Records' with:
            - primary_key = foreign_key_value
            - name = 'Inferred / Pending API Sync'
            - _is_inferred = True
            - last_modified = '1970-01-01T00:00:00Z' (epoch timestamp)
        3. Persists stubs to the Dimension table.
        4. When the real Dimension record arrives from the API later, DedupEngine
           automatically overwrites the stub because real LastModified > Epoch!

    Deep module design:
        Interface: resolve_inferred_dimensions(fact_df, fk_mappings, dim_sink_callback) -> dict
        Implementation: Distinct FK extraction, stub schema synthesis, idempotent merge.
    """

    def __init__(self, epoch_timestamp: str = "1970-01-01T00:00:00Z"):
        """Initialize Inferred Dimension Router.

        Args:
            epoch_timestamp: Base timestamp for stubs so real API records take precedence.
        """
        self.epoch_timestamp = epoch_timestamp

    def generate_inferred_stubs(
        self,
        fact_df,
        fk_column: str,
        dim_pk_column: str = "sysid",
        dim_name_column: str = "name",
        partition_col: str = "ingest_date",
        partition_val: Optional[str] = None,
    ):
        """Extract distinct foreign keys from Fact DataFrame and synthesize Inferred Dimension stubs.

        Args:
            fact_df: Incoming Fact Spark DataFrame.
            fk_column: Column name in Fact containing the FK (e.g. 'c_project').
            dim_pk_column: Target PK column in Dim table (e.g. 'sysid').
            dim_name_column: Target Name/Description column in Dim table.
            partition_col: Partition column name.
            partition_val: Current partition date string.

        Returns:
            DataFrame or None: Spark DataFrame with inferred dimension stub rows.
        """
        import pyspark.sql.functions as F

        if fk_column not in fact_df.columns:
            logger.info("FK column '%s' not present in Fact DataFrame; skipping inferred dims.", fk_column)
            return None

        today_str = partition_val or datetime.utcnow().strftime("%Y-%m-%d")

        # Extract distinct non-null FK values
        distinct_fks = (
            fact_df
            .select(F.col(fk_column).alias(dim_pk_column))
            .filter(F.col(dim_pk_column).isNotNull() & (F.length(F.trim(F.col(dim_pk_column))) > 0))
            .distinct()
        )

        if distinct_fks.rdd.isEmpty():
            return None

        # Synthesize stub dimension rows
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
            "event": "inferred_dimension_stubs_generated",
            "fk_column": fk_column,
            "stub_count": stub_count,
        }))

        return stubs_df

    def reconcile_dimension(
        self,
        stubs_df,
        dim_table_path: str,
        spark_session,
        dedup_engine,
        dim_pk_column: str = "sysid",
        partition_col: str = "ingest_date",
    ):
        """Merge inferred stubs with existing dimension table so no real records are overwritten.

        Args:
            stubs_df: Inferred stubs DataFrame from generate_inferred_stubs.
            dim_table_path: File/S3 storage path of the Dimension table.
            spark_session: Active SparkSession.
            dedup_engine: DedupEngine instance.
            dim_pk_column: Dimension PK.
            partition_col: Partition column.

        Returns:
            int: Number of stubs inserted or reconciled.
        """
        if stubs_df is None or stubs_df.rdd.isEmpty():
            return 0

        # Merge with existing dimension data using DedupEngine
        merged_dim_df = dedup_engine.merge_with_existing(
            incoming_df=stubs_df,
            existing_storage_path=dim_table_path,
            spark_session=spark_session,
            primary_key=dim_pk_column,
            watermark_col="last_modified",
            partition_col=partition_col,
        )

        # Overwrite dimension table with reconciled stubs
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
            "total_dim_records": merged_dim_df.count(),
        }))

        return stubs_df.count()
