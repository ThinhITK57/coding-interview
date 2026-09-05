import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BatchWriter:
    """
    Writes extracted batches as JSON files to a staging directory.

    Used between extraction and Spark transformation phases.
    Each batch is written as a single JSON file with all records.
    """

    def __init__(self, output_dir):
        """Initialize batch writer.

        Args:
            output_dir: Directory to write batch JSON files.
        """
        self._output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._files_written = []

    def write_batch(self, batch):
        """Write a batch of records as a JSON file.

        Args:
            batch: Batch object with records, batch_id, page_number.

        Returns:
            str: Path to the written file.
        """
        filename = f"{batch.endpoint_name}_page_{batch.page_number:06d}.json"
        file_path = os.path.join(self._output_dir, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(batch.records, f, ensure_ascii=False, default=str)

        self._files_written.append(file_path)

        logger.info(json.dumps({
            "event": "batch_written",
            "path": file_path,
            "records": batch.record_count,
            "page": batch.page_number,
        }))

        return file_path

    @property
    def files_written(self):
        return list(self._files_written)

    @property
    def output_dir(self):
        return self._output_dir


class PartitionedParquetWriter:
    """
    Writes Spark DataFrame as partitioned Parquet with Snappy compression.
    Compatible with Spark 2.3.2.

    Deep module design:
        Interface (small): write(df, output_path)
        Implementation (deep): Adds metadata columns (_ingest_date, _batch_id,
        _source_name), partitions by configured columns, writes with Snappy
        compression, handles overwrite mode.
    """

    def __init__(
        self,
        partition_columns=None,
        compression="snappy",
        source_name="",
        write_mode="append",
    ):
        """Initialize Parquet writer.

        Args:
            partition_columns: List of columns to partition by.
                Defaults to ["_ingest_date"].
            compression: Parquet compression codec. Default: "snappy".
            source_name: Source system name for metadata column.
            write_mode: Spark write mode ("append", "overwrite").
        """
        self._partition_columns = partition_columns or ["_ingest_date"]
        self._compression = compression
        self._source_name = source_name
        self._write_mode = write_mode

    def write(self, df, output_path, batch_id=""):
        """Write DataFrame as partitioned Parquet with metadata columns.

        Adds three metadata columns before writing:
        - _ingest_date: UTC date of ingestion (for partitioning)
        - _batch_id: Unique batch identifier
        - _source_name: Source system name

        Args:
            df: Spark DataFrame to write.
            output_path: Target directory for Parquet files.
            batch_id: Unique batch identifier string.

        Returns:
            dict: Write summary with record count and path.
        """
        from pyspark.sql import functions as F

        # Add metadata columns
        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
        df_with_meta = (
            df
            .withColumn("_ingest_date", F.lit(ingest_date))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_name", F.lit(self._source_name))
        )

        # Count records (Spark 2.3.2 compatible)
        record_count = df_with_meta.count()

        # Write partitioned Parquet
        (
            df_with_meta
            .write
            .mode(self._write_mode)
            .partitionBy(*self._partition_columns)
            .option("compression", self._compression)
            .parquet(output_path)
        )

        summary = {
            "output_path": output_path,
            "record_count": record_count,
            "partition_columns": self._partition_columns,
            "compression": self._compression,
            "ingest_date": ingest_date,
            "batch_id": batch_id,
        }

        logger.info(json.dumps({
            "event": "parquet_written",
            **summary,
        }))

        return summary
