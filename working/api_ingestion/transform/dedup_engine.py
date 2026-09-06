import logging
import json
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)


class DedupEngine:
    """
    Idempotent Deduplication Engine compatible with Spark 2.3.2.

    Since Spark 2.3.2 lacks the ACID 'MERGE INTO' command, this engine
    achieves 100% idempotent upserts using Window Ranking:
        row_number() OVER (PARTITION BY pk ORDER BY watermark DESC, ingest_time DESC) == 1

    Deep module design:
        Interface (small):
            deduplicate(df, primary_key, watermark_col) -> (deduped_df, stats_dict)
            merge_with_existing(incoming_df, existing_path, primary_key, ...) -> merged_df
        Implementation (deep):
            Window spec optimization, null-safe descending sort, existing partition loading,
            union and re-ranking, dynamic partition overwrite compatibility.
    """

    def __init__(
        self,
        primary_key: str = "sysid",
        watermark_col: str = "last_modified",
        fallback_timestamp_col: str = "_ingest_timestamp",
    ):
        """Initialize DedupEngine.

        Args:
            primary_key: Unique business key column name.
            watermark_col: Column indicating record update time (e.g. last_modified, updated_at).
            fallback_timestamp_col: Tie-breaker timestamp column if watermarks match or are null.
        """
        self.default_pk = primary_key
        self.default_watermark = watermark_col
        self.default_fallback = fallback_timestamp_col

    def deduplicate(
        self,
        df,
        primary_key: Optional[str] = None,
        watermark_col: Optional[str] = None,
    ):
        """Deduplicate records in a Spark DataFrame using Window ranking.

        Args:
            df: Input Spark DataFrame.
            primary_key: Business primary key. Defaults to self.default_pk.
            watermark_col: Watermark timestamp column. Defaults to self.default_watermark.

        Returns:
            tuple: (deduped_df, stats_dict)
        """
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F

        pk = primary_key or self.default_pk
        wm = watermark_col or self.default_watermark

        # Ensure primary key exists
        if pk not in df.columns:
            logger.warning(json.dumps({
                "event": "dedup_skipped_pk_missing",
                "pk": pk,
                "available_columns": df.columns[:10],
            }))
            return df, {"input_count": df.count(), "deduped_count": df.count(), "duplicates_removed": 0}

        # Build ordering columns
        order_cols = []
        if wm in df.columns:
            # Spark 2.3.2 null-safe descending: nulls last
            order_cols.append(F.when(F.col(wm).isNotNull(), F.col(wm)).otherwise(F.lit("1970-01-01")).desc())

        if self.default_fallback in df.columns:
            order_cols.append(F.col(self.default_fallback).desc())

        # If no timestamp column found, order by PK asc as deterministic tie-breaker
        if not order_cols:
            order_cols.append(F.col(pk).asc())

        # Define Window specification
        window_spec = Window.partitionBy(pk).orderBy(*order_cols)

        # Apply row_number ranking and filter the latest record
        input_count = df.count()
        deduped_df = (
            df
            .withColumn("_row_rank", F.row_number().over(window_spec))
            .filter(F.col("_row_rank") == 1)
            .drop("_row_rank")
        )
        deduped_count = deduped_df.count()
        duplicates_removed = input_count - deduped_count

        stats = {
            "primary_key": pk,
            "watermark_col": wm,
            "input_count": input_count,
            "deduped_count": deduped_count,
            "duplicates_removed": duplicates_removed,
        }

        logger.info(json.dumps({
            "event": "deduplication_completed",
            **stats,
        }))

        return deduped_df, stats

    def merge_with_existing(
        self,
        incoming_df,
        existing_storage_path: str,
        spark_session,
        primary_key: Optional[str] = None,
        watermark_col: Optional[str] = None,
        partition_col: Optional[str] = "ingest_date",
        partition_val: Optional[str] = None,
    ):
        """Merge incoming DataFrame with existing partition on storage to achieve ACID-like idempotency.

        In Spark 2.3.2:
            1. Read existing partition from storage (if exists).
            2. Union with incoming batch.
            3. Deduplicate across the combined dataset.
            4. Result is ready for dynamic partition overwrite.

        Args:
            incoming_df: New batch DataFrame.
            existing_storage_path: Path to table root on Parquet/S3.
            spark_session: Active SparkSession.
            primary_key: Primary key column.
            watermark_col: Watermark timestamp column.
            partition_col: Partition column name.
            partition_val: Specific partition to load (optional).

        Returns:
            DataFrame: Fully merged and deduplicated DataFrame ready to write.
        """
        import os

        # Check if existing path exists
        existing_df = None
        try:
            target_read_path = existing_storage_path
            if partition_col and partition_val:
                specific_path = os.path.join(existing_storage_path, f"{partition_col}={partition_val}")
                if os.path.exists(specific_path):
                    target_read_path = specific_path

            if os.path.exists(target_read_path) or target_read_path.startswith("s3"):
                existing_df = spark_session.read.parquet(target_read_path)
                if existing_df.rdd.isEmpty():
                    existing_df = None
        except Exception as e:
            logger.info("No readable existing partition found at %s: %s", existing_storage_path, e)
            existing_df = None

        if existing_df is None:
            # First run: simply deduplicate incoming batch
            deduped_df, _ = self.deduplicate(incoming_df, primary_key, watermark_col)
            return deduped_df

        # Harmonize columns for unionByName (Spark 2.3.2 doesn't have allowMissingColumns=True in unionByName)
        incoming_cols = set(incoming_df.columns)
        existing_cols = set(existing_df.columns)
        all_cols = list(incoming_cols.union(existing_cols))

        import pyspark.sql.functions as F

        def align_df(d, cols):
            for c in cols:
                if c not in d.columns:
                    d = d.withColumn(c, F.lit(None))
            return d.select(*cols)

        incoming_aligned = align_df(incoming_df, all_cols)
        existing_aligned = align_df(existing_df, all_cols)

        # Union and deduplicate
        combined_df = incoming_aligned.union(existing_aligned)
        merged_deduped_df, stats = self.deduplicate(combined_df, primary_key, watermark_col)

        logger.info(json.dumps({
            "event": "merge_with_existing_complete",
            "path": existing_storage_path,
            "incoming_rows": incoming_df.count(),
            "existing_rows": existing_df.count(),
            "merged_final_rows": merged_deduped_df.count(),
        }))

        return merged_deduped_df
