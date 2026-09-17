import os
import json
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

class BatchWriter:
    def __init__(self, output_dir):
        self._output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._files_written = []

    def write_batch(self, batch):
        filename = f"{batch.endpoint_name}_page_{batch.page_number:06d}.json"
        file_path = os.path.join(self._output_dir, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(batch.records, f, ensure_ascii=False, default=str)

        self._files_written.append(file_path)

        logger.info(json.dumps({
            "event": "batch_written",
            "path": file_path,
            "records": batch.record_count,
            "page": batch.page_number
        }))

        return file_path

    @property
    def files_written(self):
        return list(self._files_written)

    @property
    def output_dir(self):
        return self._output_dir


class PartitionedParquetWriter:
    def __init__(self, partition_columns=None, compression='snappy', source_name="", write_mode="append"):
        self._partition_columns = partition_columns or ["ingest_date"]
        self._compression = compression
        self._source_name = source_name
        self._write_mode = write_mode


    def write(self, df, output_path, batch_id=""):
        from pyspark.sql import functions as F

        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
        df_with_meta = (
            df
            .withColumn("ingest_date", F.lit(ingest_date))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_name", F.lit(self._source_name))
        )

        # df_with_meta.cache()
        record_count = df_with_meta.count()

        (
            df_with_meta
            .write
            .mode(self._write_mode)
            .partitionBy("ingest_date")     #.partitionBy(*self._partition_columns)
            .option("compression", self._compression)
            .parquet(output_path)
        )
        summary = {
            "output_path": output_path,
            "record_count": record_count,
            "partition_columns": self._partition_columns,
            "compression": self._compression,
            "ingest_date": ingest_date,
            "batch_id": batch_id
        }
        logger.info(json.dumps({
            "event": "parquet_written",
            **summary
        }))
        # df_with_meta.unpersist()

        return summary
        