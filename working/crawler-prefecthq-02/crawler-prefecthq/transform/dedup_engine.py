import logging
import json
from typing import Optional, List, Tuple


logger = logging.getLogger(__name__)


class DedupEngine:
    def __init__(self, primary_key, watermark_col: str = "last_modified", fallback_timestamp_col: str = "_ingest_timestamp"):
        self.default_pk = primary_key
        self.default_watermark = watermark_col
        self.default_fallback = fallback_timestamp_col

    def deduplicate(
            self,
            df,
            primary_key: Optional[str] = None,
            watermark_col: Optional[str] = None
    ):
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F

        pk = primary_key or self.default_pk
        wm = watermark_col or self.default_watermark

        if pk not in df.columns:
            logger.warning(json.dumps({
                "event": "dedup_skipped_pk_missing",
                "pk": pk,
                "available_columns": df.columns[:10]
            }))
            input_count = df.count()
            return df, {
                "primary_key": pk,
                "watermark_col": wm,
                "input_count": input_count,
                "deduped_count": input_count,
                "duplicates_removed": 0,
                "status": "SKIPPED_PK_MISSING"
            }

        order_cols = []
        if wm in df.columns:
            # DESC trong Spark mac dinh xep NULL xuong cuoi, tuc ban ghi thieu
            # watermark tu dong bi coi la cu nhat. Khong dung moc
            # "1970-01-01" nua: literal chuoi do ep ca bieu thuc ve string khi
            # watermark la cot DATE, va sap xep sai neu watermark la kieu so.
            order_cols.append(F.col(wm).desc())

        if self.default_fallback in df.columns:
            order_cols.append(F.col(self.default_fallback).desc())

        if not order_cols:
            order_cols.append(F.col(pk).asc())

        window_spec = Window.partitionBy(pk).orderBy(*order_cols)
        # input_count = df.count()
        deduped_df = (
            df
            .withColumn("_row_rank", F.row_number().over(window_spec))
            .filter(F.col("_row_rank") == 1)
            .drop("_row_rank")
        )


        input_count = df.count()
        deduped_count = deduped_df.count()
        duplicates_removed = (input_count - deduped_count)

        stats = {
            "primary_key": pk,
            "watermark_col": wm,
            "input_count": input_count,
            "deduped_count": deduped_count,
            "duplicates_removed": duplicates_removed,
            "status": "COMPLETED"
        }
        logger.info(json.dumps({
            "event": "deduplication_completed",
            **stats
        }))

        return deduped_df, stats

    def merge_with_existing(
            self,
            incoming_df,
            existing_storage_path: str,
            spark_session, 
            primary_key: Optional[str]=None,
            watermark_col: Optional[str]=None,
            partition_col: Optional[str]="ingest_date",
            partition_val: Optional[str] = None 
    ):
        import os
        existing_df = None
        try:
            target_read_path = existing_storage_path
            if partition_col and partition_val:
                specific_path = os.path.join(existing_storage_path, f"{partition_col}={partition_val}")
                if os.path.exists(specific_path):
                    target_read_path = specific_path

            # Local path
            local_exists = os.path.exists(target_read_path)
            # S3/S3A path
            remote_path = (
                target_read_path.startswith("s3://")
                or target_read_path.startswith("s3a://")
            )
            if local_exists or remote_path:

                existing_df = (
                    spark_session
                    .read
                    .parquet(target_read_path)
                )
            # if os.path.exists(target_read_path) or target_read_path.startswith("s3"):
            #     existing_df = spark_session.read.parquet(target_read_path)
            #     if existing_df.rdd.isEmpty():
            #         existing_df = None
        except Exception as e:
            logger.info("No readable existing partition found at %s: %s", existing_storage_path, e)
            existing_df = None

        if existing_df is None:
            deduped_df, _ = self.deduplicate(incoming_df, primary_key, watermark_col)
            return deduped_df

        incoming_cols = set(incoming_df.columns)
        existing_cols = set(existing_df.columns)
        all_cols = list(incoming_cols.union(existing_cols))

        import pyspark.sql.functions as F

        def align_df(d, cols):
            existing_columns = set(d.columns)

            for c in cols:
                if c not in existing_columns:
                    d = d.withColumns(c, F.lit(None))
            return d.select(*cols)

        incoming_aligned = align_df(incoming_df, all_cols)
        existing_aligned = align_df(existing_df, all_cols)

        combined_df = incoming_aligned.union(existing_aligned)
        merged_deduped_df, stats = self.deduplicate(combined_df, primary_key=primary_key, watermark_col=watermark_col)

        logger.info(json.dumps({
            "event": "merge_with_existing_complete",
            "path": existing_storage_path,
            "input_count": stats.get("input_count"),
            "merged_final_rows": stats.get("deduped_count"),
                "duplicates_removed": stats.get(
                    "duplicates_removed"
                )
        }))

        return merged_deduped_df

